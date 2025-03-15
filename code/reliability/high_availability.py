"""
High availability implementation for the distributed platform.
"""
import asyncio
import json
import time
import random
from typing import Dict, List, Optional, Set, Any, Tuple
import uuid

from ..common.models import NodeInfo, LoadMetrics
from ..common.utils import get_logger, Timer
from ..messaging.messaging import MessageBroker, MessageConsumer, MessageProducer

logger = get_logger(__name__)

class HeartbeatMonitor:
    """Heartbeat monitor for detecting node failures."""
    
    def __init__(self, broker: MessageBroker, heartbeat_interval: float = 5.0,
                failure_threshold: int = 3):
        """
        Initialize heartbeat monitor.
        
        Args:
            broker: Message broker
            heartbeat_interval: Interval between heartbeats in seconds
            failure_threshold: Number of missed heartbeats before node is considered failed
        """
        self.broker = broker
        self.heartbeat_interval = heartbeat_interval
        self.failure_threshold = failure_threshold
        
        # Messaging
        self.heartbeat_producer = MessageProducer(broker, default_topic="node-heartbeats")
        self.heartbeat_consumer = MessageConsumer(
            broker=broker,
            topics=["node-heartbeats"],
            group_id="heartbeat-monitor"
        )
        
        # State
        self.node_last_heartbeat: Dict[str, float] = {}
        self.node_missed_heartbeats: Dict[str, int] = {}
        self.node_status: Dict[str, str] = {}  # "active", "suspected", "failed"
        self.failure_callbacks: List[callable] = []
        self.recovery_callbacks: List[callable] = []
        
        # Tasks
        self.running = False
        self.monitoring_task = None
    
    async def start(self):
        """Start the heartbeat monitor."""
        if self.running:
            return
        
        self.running = True
        
        # Start consumer
        await self.heartbeat_consumer.start()
        self.heartbeat_consumer.on_message("node-heartbeats", self._handle_heartbeat)
        
        # Start monitoring task
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())
        
        logger.info("Heartbeat monitor started")
    
    async def stop(self):
        """Stop the heartbeat monitor."""
        if not self.running:
            return
        
        self.running = False
        
        # Stop monitoring task
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
        
        # Stop consumer
        await self.heartbeat_consumer.stop()
        
        logger.info("Heartbeat monitor stopped")
    
    def register_node(self, node_id: str):
        """
        Register a node for monitoring.
        
        Args:
            node_id: Node ID
        """
        self.node_last_heartbeat[node_id] = time.time()
        self.node_missed_heartbeats[node_id] = 0
        self.node_status[node_id] = "active"
        logger.info(f"Registered node {node_id} for heartbeat monitoring")
    
    def unregister_node(self, node_id: str):
        """
        Unregister a node from monitoring.
        
        Args:
            node_id: Node ID
        """
        if node_id in self.node_last_heartbeat:
            del self.node_last_heartbeat[node_id]
        if node_id in self.node_missed_heartbeats:
            del self.node_missed_heartbeats[node_id]
        if node_id in self.node_status:
            del self.node_status[node_id]
        logger.info(f"Unregistered node {node_id} from heartbeat monitoring")
    
    def on_node_failure(self, callback: callable):
        """
        Register callback for node failure.
        
        Args:
            callback: Callback function(node_id: str)
        """
        self.failure_callbacks.append(callback)
    
    def on_node_recovery(self, callback: callable):
        """
        Register callback for node recovery.
        
        Args:
            callback: Callback function(node_id: str)
        """
        self.recovery_callbacks.append(callback)
    
    async def send_heartbeat(self, node_id: str, load_metrics: Optional[LoadMetrics] = None):
        """
        Send heartbeat for a node.
        
        Args:
            node_id: Node ID
            load_metrics: Optional load metrics
            
        Returns:
            True if heartbeat was sent, False otherwise
        """
        heartbeat = {
            "node_id": node_id,
            "timestamp": time.time(),
            "load_metrics": load_metrics.__dict__ if load_metrics else None
        }
        
        return await self.heartbeat_producer.send(heartbeat, key=node_id)
    
    def _handle_heartbeat(self, message):
        """
        Handle heartbeat message.
        
        Args:
            message: Heartbeat message
        """
        try:
            data = message.value
            node_id = data["node_id"]
            timestamp = data["timestamp"]
            
            # Update last heartbeat
            self.node_last_heartbeat[node_id] = timestamp
            self.node_missed_heartbeats[node_id] = 0
            
            # Check if node was previously failed
            if node_id in self.node_status and self.node_status[node_id] == "failed":
                self.node_status[node_id] = "active"
                logger.info(f"Node {node_id} recovered")
                
                # Notify recovery callbacks
                for callback in self.recovery_callbacks:
                    try:
                        callback(node_id)
                    except Exception as e:
                        logger.error(f"Error in recovery callback: {e}")
            elif node_id not in self.node_status:
                # New node
                self.node_status[node_id] = "active"
                logger.info(f"New node {node_id} detected")
        except Exception as e:
            logger.error(f"Error handling heartbeat: {e}")
    
    async def _monitoring_loop(self):
        """Monitoring loop for detecting node failures."""
        while self.running:
            try:
                now = time.time()
                
                # Check for missed heartbeats
                for node_id in list(self.node_last_heartbeat.keys()):
                    last_heartbeat = self.node_last_heartbeat[node_id]
                    elapsed = now - last_heartbeat
                    
                    if elapsed > self.heartbeat_interval:
                        # Increment missed heartbeats
                        self.node_missed_heartbeats[node_id] += 1
                        
                        if self.node_missed_heartbeats[node_id] >= self.failure_threshold:
                            # Node failed
                            if node_id in self.node_status and self.node_status[node_id] != "failed":
                                self.node_status[node_id] = "failed"
                                logger.warning(f"Node {node_id} failed after {self.node_missed_heartbeats[node_id]} missed heartbeats")
                                
                                # Notify failure callbacks
                                for callback in self.failure_callbacks:
                                    try:
                                        callback(node_id)
                                    except Exception as e:
                                        logger.error(f"Error in failure callback: {e}")
                
                # Sleep until next check
                await asyncio.sleep(self.heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(1.0)


class LeaderElection:
    """Leader election for high availability."""
    
    def __init__(self, broker: MessageBroker, node_id: str, 
                election_timeout: float = 10.0):
        """
        Initialize leader election.
        
        Args:
            broker: Message broker
            node_id: This node's ID
            election_timeout: Election timeout in seconds
        """
        self.broker = broker
        self.node_id = node_id
        self.election_timeout = election_timeout
        
        # Messaging
        self.election_producer = MessageProducer(broker, default_topic="leader-election")
        self.election_consumer = MessageConsumer(
            broker=broker,
            topics=["leader-election"],
            group_id=f"election-{node_id}"
        )
        
        # State
        self.term = 0
        self.leader_id = None
        self.voted_for = None
        self.is_leader = False
        self.votes_received = set()
        self.known_nodes = set()
        self.election_timer = None
        self.leadership_callbacks: List[callable] = []
        self.follower_callbacks: List[callable] = []
        
        # Tasks
        self.running = False
        self.election_task = None
        self.heartbeat_task = None
    
    async def start(self):
        """Start leader election."""
        if self.running:
            return
        
        self.running = True
        
        # Start consumer
        await self.election_consumer.start()
        self.election_consumer.on_message("leader-election", self._handle_election_message)
        
        # Start election task
        self.election_task = asyncio.create_task(self._election_loop())
        
        logger.info(f"Leader election started for node {self.node_id}")
    
    async def stop(self):
        """Stop leader election."""
        if not self.running:
            return
        
        self.running = False
        
        # Stop tasks
        if self.election_task:
            self.election_task.cancel()
            try:
                await self.election_task
            except asyncio.CancelledError:
                pass
        
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
        
        # Stop consumer
        await self.election_consumer.stop()
        
        logger.info(f"Leader election stopped for node {self.node_id}")
    
    def on_become_leader(self, callback: callable):
        """
        Register callback for becoming leader.
        
        Args:
            callback: Callback function()
        """
        self.leadership_callbacks.append(callback)
    
    def on_become_follower(self, callback: callable):
        """
        Register callback for becoming follower.
        
        Args:
            callback: Callback function(leader_id: str)
        """
        self.follower_callbacks.append(callback)
    
    def add_node(self, node_id: str):
        """
        Add node to known nodes.
        
        Args:
            node_id: Node ID
        """
        self.known_nodes.add(node_id)
    
    def remove_node(self, node_id: str):
        """
        Remove node from known nodes.
        
        Args:
            node_id: Node ID
        """
        if node_id in self.known_nodes:
            self.known_nodes.remove(node_id)
        
        if node_id in self.votes_received:
            self.votes_received.remove(node_id)
        
        if self.leader_id == node_id:
            # Leader failed, start new election
            self.leader_id = None
            self.is_leader = False
            self._reset_election_timer()
    
    def is_current_leader(self) -> bool:
        """
        Check if this node is the current leader.
        
        Returns:
            True if this node is the leader, False otherwise
        """
        return self.is_leader
    
    def get_leader_id(self) -> Optional[str]:
        """
        Get current leader ID.
        
        Returns:
            Leader ID or None if no leader
        """
        return self.leader_id
    
    async def _send_vote_request(self):
        """Send vote request."""
        message = {
            "type": "vote_request",
            "term": self.term,
            "candidate_id": self.node_id,
            "timestamp": time.time()
        }
        
        await self.election_producer.send(message, key=self.node_id)
        logger.debug(f"Sent vote request for term {self.term}")
    
    async def _send_vote_response(self, candidate_id: str, granted: bool):
        """
        Send vote response.
        
        Args:
            candidate_id: Candidate ID
            granted: Whether vote was granted
        """
        message = {
            "type": "vote_response",
            "term": self.term,
            "voter_id": self.node_id,
            "candidate_id": candidate_id,
            "granted": granted,
            "timestamp": time.time()
        }
        
        await self.election_producer.send(message, key=self.node_id)
        logger.debug(f"Sent vote response to {candidate_id}, granted={granted}")
    
    async def _send_leader_heartbeat(self):
        """Send leader heartbeat."""
        message = {
            "type": "leader_heartbeat",
            "term": self.term,
            "leader_id": self.node_id,
            "timestamp": time.time()
        }
        
        await self.election_producer.send(message, key=self.node_id)
        logger.debug(f"Sent leader heartbeat for term {self.term}")
    
    def _handle_election_message(self, message):
        """
        Handle election message.
        
        Args:
            message: Election message
        """
        try:
            data = message.value
            msg_type = data["type"]
            msg_term = data["term"]
            
            # Update term if message term is higher
            if msg_term > self.term:
                self.term = msg_term
                self.voted_for = None
                
                if self.is_leader:
                    self.is_leader = False
                    self._notify_follower_callbacks(None)
            
            if msg_type == "vote_request":
                self._handle_vote_request(data)
            elif msg_type == "vote_response":
                self._handle_vote_response(data)
            elif msg_type == "leader_heartbeat":
                self._handle_leader_heartbeat(data)
        except Exception as e:
            logger.error(f"Error handling election message: {e}")
    
    def _handle_vote_request(self, data):
        """
        Handle vote request.
        
        Args:
            data: Vote request data
        """
        candidate_id = data["candidate_id"]
        msg_term = data["term"]
        
        # Decide whether to grant vote
        grant_vote = False
        
        if msg_term >= self.term and (self.voted_for is None or self.voted_for == candidate_id):
            grant_vote = True
            self.voted_for = candidate_id
            self.term = msg_term
            
            # Reset election timer
            self._reset_election_timer()
        
        # Send vote response
        asyncio.create_task(self._send_vote_response(candidate_id, grant_vote))
    
    def _handle_vote_response(self, data):
        """
        Handle vote response.
        
        Args:
            data: Vote response data
        """
        voter_id = data["voter_id"]
        candidate_id = data["candidate_id"]
        granted = data["granted"]
        msg_term = data["term"]
        
        # Ignore if not for this node or old term
        if candidate_id != self.node_id or msg_term != self.term:
            return
        
        if granted:
            self.votes_received.add(voter_id)
            
            # Check if majority
            if len(self.votes_received) > len(self.known_nodes) / 2:
                if not self.is_leader:
                    self.is_leader = True
                    self.leader_id = self.node_id
                    logger.info(f"Node {self.node_id} elected as leader for term {self.term}")
                    
                    # Start leader heartbeat
                    if self.heartbeat_task:
                        self.heartbeat_task.cancel()
                    self.heartbeat_task = asyncio.create_task(self._leader_heartbeat_loop())
                    
                    # Notify leadership callbacks
                    self._notify_leadership_callbacks()
    
    def _handle_leader_heartbeat(self, data):
        """
        Handle leader heartbeat.
        
        Args:
            data: Leader heartbeat data
        """
        leader_id = data["leader_id"]
        msg_term = data["term"]
        
        if msg_term >= self.term:
            # Accept leader
            old_leader = self.leader_id
            self.term = msg_term
            self.leader_id = leader_id
            self.voted_for = None
            
            if self.is_leader and leader_id != self.node_id:
                self.is_leader = False
                logger.info(f"Stepping down as leader, new leader is {leader_id}")
                
                # Stop leader heartbeat
                if self.heartbeat_task:
                    self.heartbeat_task.cancel()
                    self.heartbeat_task = None
            
            # Reset election timer
            self._reset_election_timer()
            
            # Notify follower callbacks if leader changed
            if old_leader != leader_id:
                self._notify_follower_callbacks(leader_id)
    
    def _reset_election_timer(self):
        """Reset election timer."""
        if self.election_timer:
            self.election_timer.cancel()
        
        # Random timeout to avoid split votes
        timeout = self.election_timeout * (1 + random.random())
        self.election_timer = asyncio.get_event_loop().call_later(
            timeout, self._election_timeout
        )
    
    def _election_timeout(self):
        """Election timeout callback."""
        if not self.running:
            return
        
        if not self.is_leader:
            # Start election
            self.term += 1
            self.voted_for = self.node_id
            self.votes_received = {self.node_id}  # Vote for self
            
            logger.info(f"Starting election for term {self.term}")
            asyncio.create_task(self._send_vote_request())
            
            # Reset election timer
            self._reset_election_timer()
    
    async def _election_loop(self):
        """Main election loop."""
        # Initial election timeout
        self._reset_election_timer()
        
        while self.running:
            try:
                # Just keep the task alive
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in election loop: {e}")
                await asyncio.sleep(1.0)
    
    async def _leader_heartbeat_loop(self):
        """Leader heartbeat loop."""
        while self.running and self.is_leader:
            try:
                await self._send_leader_heartbeat()
                await asyncio.sleep(self.election_timeout / 2)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in leader heartbeat loop: {e}")
                await asyncio.sleep(1.0)
    
    def _notify_leadership_callbacks(self):
        """Notify leadership callbacks."""
        for callback in self.leadership_callbacks:
            try:
                callback()
            except Exception as e:
                logger.error(f"Error in leadership callback: {e}")
    
    def _notify_follower_callbacks(self, leader_id: Optional[str]):
        """
        Notify follower callbacks.
        
        Args:
            leader_id: Leader ID or None if no leader
        """
        for callback in self.follower_callbacks:
            try:
                callback(leader_id)
            except Exception as e:
                logger.error(f"Error in follower callback: {e}")


class ReplicationManager:
    """Manager for data replication across nodes."""
    
    def __init__(self, broker: MessageBroker, node_id: str, 
                replication_factor: int = 3):
        """
        Initialize replication manager.
        
        Args:
            broker: Message broker
            node_id: This node's ID
            replication_factor: Number of replicas for each data item
        """
        self.broker = broker
        self.node_id = node_id
        self.replication_factor = replication_factor
        
        # Messaging
        self.replication_producer = MessageProducer(broker, default_topic="data-replication")
        self.replication_consumer = MessageConsumer(
            broker=broker,
            topics=["data-replication"],
            group_id=f"replication-{node_id}"
        )
        
        # State
        self.data_store = {}  # Local data store
        self.known_nodes = []  # List of known nodes
        self.leader_election = None  # Leader election instance
        
        # Tasks
        self.running = False
        self.replication_task = None
    
    async def start(self):
        """Start replication manager."""
        if self.running:
            return
        
        self.running = True
        
        # Start consumer
        await self.replication_consumer.start()
        self.replication_consumer.on_message("data-replication", self._handle_replication_message)
        
        # Start replication task
        self.replication_task = asyncio.create_task(self._replication_loop())
        
        logger.info(f"Replication manager started for node {self.node_id}")
    
    async def stop(self):
        """Stop replication manager."""
        if not self.running:
            return
        
        self.running = False
        
        # Stop replication task
        if self.replication_task:
            self.replication_task.cancel()
            try:
                await self.replication_task
            except asyncio.CancelledError:
                pass
        
        # Stop consumer
        await self.replication_consumer.stop()
        
        logger.info(f"Replication manager stopped for node {self.node_id}")
    
    def set_leader_election(self, leader_election: LeaderElection):
        """
        Set leader election instance.
        
        Args:
            leader_election: Leader election instance
        """
        self.leader_election = leader_election
    
    def update_known_nodes(self, nodes: List[str]):
        """
        Update list of known nodes.
        
        Args:
            nodes: List of node IDs
        """
        self.known_nodes = nodes
    
    async def store_data(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Store data with replication.
        
        Args:
            key: Data key
            value: Data value
            ttl: Time-to-live in seconds (optional)
            
        Returns:
            True if data was stored, False otherwise
        """
        # Store locally
        self.data_store[key] = {
            "value": value,
            "timestamp": time.time(),
            "ttl": ttl,
            "expires": time.time() + ttl if ttl else None
        }
        
        # If leader, replicate to other nodes
        if self.leader_election and self.leader_election.is_current_leader():
            await self._replicate_data(key)
        
        return True
    
    async def get_data(self, key: str) -> Optional[Any]:
        """
        Get data.
        
        Args:
            key: Data key
            
        Returns:
            Data value or None if not found or expired
        """
        if key not in self.data_store:
            return None
        
        item = self.data_store[key]
        
        # Check if expired
        if item["expires"] and time.time() > item["expires"]:
            del self.data_store[key]
            return None
        
        return item["value"]
    
    async def delete_data(self, key: str) -> bool:
        """
        Delete data.
        
        Args:
            key: Data key
            
        Returns:
            True if data was deleted, False otherwise
        """
        if key in self.data_store:
            del self.data_store[key]
            
            # If leader, replicate deletion to other nodes
            if self.leader_election and self.leader_election.is_current_leader():
                await self._replicate_deletion(key)
            
            return True
        return False
    
    async def _replicate_data(self, key: str):
        """
        Replicate data to other nodes.
        
        Args:
            key: Data key
        """
        if key not in self.data_store:
            return
        
        item = self.data_store[key]
        
        message = {
            "type": "replicate",
            "key": key,
            "value": item["value"],
            "timestamp": item["timestamp"],
            "ttl": item["ttl"],
            "source_node": self.node_id
        }
        
        await self.replication_producer.send(message, key=key)
        logger.debug(f"Sent replication message for key {key}")
    
    async def _replicate_deletion(self, key: str):
        """
        Replicate deletion to other nodes.
        
        Args:
            key: Data key
        """
        message = {
            "type": "delete",
            "key": key,
            "timestamp": time.time(),
            "source_node": self.node_id
        }
        
        await self.replication_producer.send(message, key=key)
        logger.debug(f"Sent deletion message for key {key}")
    
    def _handle_replication_message(self, message):
        """
        Handle replication message.
        
        Args:
            message: Replication message
        """
        try:
            data = message.value
            msg_type = data["type"]
            key = data["key"]
            source_node = data["source_node"]
            
            # Ignore own messages
            if source_node == self.node_id:
                return
            
            if msg_type == "replicate":
                self._handle_replicate(data)
            elif msg_type == "delete":
                self._handle_delete(data)
        except Exception as e:
            logger.error(f"Error handling replication message: {e}")
    
    def _handle_replicate(self, data):
        """
        Handle replicate message.
        
        Args:
            data: Replicate message data
        """
        key = data["key"]
        value = data["value"]
        timestamp = data["timestamp"]
        ttl = data["ttl"]
        
        # Check if newer than existing data
        if key in self.data_store and self.data_store[key]["timestamp"] >= timestamp:
            return
        
        # Store data
        self.data_store[key] = {
            "value": value,
            "timestamp": timestamp,
            "ttl": ttl,
            "expires": time.time() + ttl if ttl else None
        }
        
        logger.debug(f"Replicated data for key {key}")
    
    def _handle_delete(self, data):
        """
        Handle delete message.
        
        Args:
            data: Delete message data
        """
        key = data["key"]
        timestamp = data["timestamp"]
        
        # Check if newer than existing data
        if key in self.data_store and self.data_store[key]["timestamp"] >= timestamp:
            return
        
        # Delete data
        if key in self.data_store:
            del self.data_store[key]
            logger.debug(f"Deleted replicated data for key {key}")
    
    async def _replication_loop(self):
        """Main replication loop."""
        while self.running:
            try:
                # If leader, periodically check for unreplicated data
                if self.leader_election and self.leader_election.is_current_leader():
                    for key in self.data_store:
                        await self._replicate_data(key)
                
                # Sleep
                await asyncio.sleep(10.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in replication loop: {e}")
                await asyncio.sleep(1.0)
