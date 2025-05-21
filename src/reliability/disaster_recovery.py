"""
Disaster recovery implementation for the distributed platform.
"""
import asyncio
import json
import os
import time
import shutil
from typing import Dict, List, Optional, Set, Any, Tuple
import uuid

from ..common.models import NodeInfo
from ..common.utils import get_logger, Timer
from ..messaging.messaging import MessageBroker, MessageConsumer, MessageProducer

logger = get_logger(__name__)

class BackupManager:
    """Manager for data backups and recovery."""
    
    def __init__(self, backup_dir: str, backup_interval: float = 3600.0):
        """
        Initialize backup manager.
        
        Args:
            backup_dir: Directory for backups
            backup_interval: Interval between backups in seconds
        """
        self.backup_dir = backup_dir
        self.backup_interval = backup_interval
        
        # Ensure backup directory exists
        os.makedirs(backup_dir, exist_ok=True)
        
        # State
        self.data_sources = {}  # name -> get_data_func
        self.running = False
        self.backup_task = None
        self.last_backup_time = 0
    
    async def start(self):
        """Start backup manager."""
        if self.running:
            return
        
        self.running = True
        
        # Start backup task
        self.backup_task = asyncio.create_task(self._backup_loop())
        
        logger.info(f"Backup manager started with backup directory {self.backup_dir}")
    
    async def stop(self):
        """Stop backup manager."""
        if not self.running:
            return
        
        self.running = False
        
        # Stop backup task
        if self.backup_task:
            self.backup_task.cancel()
            try:
                await self.backup_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Backup manager stopped")
    
    def register_data_source(self, name: str, get_data_func):
        """
        Register data source for backup.
        
        Args:
            name: Data source name
            get_data_func: Function to get data for backup
        """
        self.data_sources[name] = get_data_func
        logger.info(f"Registered data source {name} for backup")
    
    def unregister_data_source(self, name: str):
        """
        Unregister data source.
        
        Args:
            name: Data source name
        """
        if name in self.data_sources:
            del self.data_sources[name]
            logger.info(f"Unregistered data source {name} from backup")
    
    async def create_backup(self) -> str:
        """
        Create backup of all registered data sources.
        
        Returns:
            Backup ID
        """
        backup_id = str(uuid.uuid4())
        backup_time = time.time()
        backup_path = os.path.join(self.backup_dir, f"backup_{backup_id}")
        
        os.makedirs(backup_path, exist_ok=True)
        
        # Backup metadata
        metadata = {
            "backup_id": backup_id,
            "timestamp": backup_time,
            "sources": list(self.data_sources.keys())
        }
        
        with open(os.path.join(backup_path, "metadata.json"), "w") as f:
            json.dump(metadata, f)
        
        # Backup data sources
        for name, get_data_func in self.data_sources.items():
            try:
                data = await get_data_func()
                
                with open(os.path.join(backup_path, f"{name}.json"), "w") as f:
                    json.dump(data, f)
                
                logger.debug(f"Backed up data source {name}")
            except Exception as e:
                logger.error(f"Error backing up data source {name}: {e}")
        
        self.last_backup_time = backup_time
        logger.info(f"Created backup {backup_id}")
        
        return backup_id
    
    async def restore_backup(self, backup_id: str, restore_funcs: Dict[str, callable]) -> bool:
        """
        Restore from backup.
        
        Args:
            backup_id: Backup ID
            restore_funcs: Dict of data source name -> restore function
            
        Returns:
            True if restored successfully, False otherwise
        """
        backup_path = os.path.join(self.backup_dir, f"backup_{backup_id}")
        
        if not os.path.exists(backup_path):
            logger.error(f"Backup {backup_id} not found")
            return False
        
        # Read metadata
        try:
            with open(os.path.join(backup_path, "metadata.json"), "r") as f:
                metadata = json.load(f)
        except Exception as e:
            logger.error(f"Error reading backup metadata: {e}")
            return False
        
        # Restore data sources
        for name, restore_func in restore_funcs.items():
            data_file = os.path.join(backup_path, f"{name}.json")
            
            if not os.path.exists(data_file):
                logger.warning(f"Data file for source {name} not found in backup")
                continue
            
            try:
                with open(data_file, "r") as f:
                    data = json.load(f)
                
                await restore_func(data)
                logger.debug(f"Restored data source {name}")
            except Exception as e:
                logger.error(f"Error restoring data source {name}: {e}")
        
        logger.info(f"Restored from backup {backup_id}")
        return True
    
    def list_backups(self) -> List[Dict[str, Any]]:
        """
        List available backups.
        
        Returns:
            List of backup metadata
        """
        backups = []
        
        for item in os.listdir(self.backup_dir):
            if item.startswith("backup_"):
                backup_path = os.path.join(self.backup_dir, item)
                
                if os.path.isdir(backup_path):
                    metadata_file = os.path.join(backup_path, "metadata.json")
                    
                    if os.path.exists(metadata_file):
                        try:
                            with open(metadata_file, "r") as f:
                                metadata = json.load(f)
                            
                            backups.append(metadata)
                        except Exception as e:
                            logger.error(f"Error reading backup metadata: {e}")
        
        # Sort by timestamp (newest first)
        backups.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
        
        return backups
    
    def delete_backup(self, backup_id: str) -> bool:
        """
        Delete backup.
        
        Args:
            backup_id: Backup ID
            
        Returns:
            True if deleted, False otherwise
        """
        backup_path = os.path.join(self.backup_dir, f"backup_{backup_id}")
        
        if not os.path.exists(backup_path):
            logger.error(f"Backup {backup_id} not found")
            return False
        
        try:
            shutil.rmtree(backup_path)
            logger.info(f"Deleted backup {backup_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting backup {backup_id}: {e}")
            return False
    
    def cleanup_old_backups(self, max_backups: int = 5):
        """
        Clean up old backups, keeping only the most recent ones.
        
        Args:
            max_backups: Maximum number of backups to keep
        """
        backups = self.list_backups()
        
        if len(backups) <= max_backups:
            return
        
        # Delete oldest backups
        for backup in backups[max_backups:]:
            self.delete_backup(backup["backup_id"])
    
    async def _backup_loop(self):
        """Main backup loop."""
        while self.running:
            try:
                now = time.time()
                
                # Check if it's time for backup
                if now - self.last_backup_time >= self.backup_interval:
                    await self.create_backup()
                    self.cleanup_old_backups()
                
                # Sleep until next check
                await asyncio.sleep(60.0)  # Check every minute
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in backup loop: {e}")
                await asyncio.sleep(60.0)


class StateTransferManager:
    """Manager for transferring state between nodes."""
    
    def __init__(self, broker: MessageBroker, node_id: str):
        """
        Initialize state transfer manager.
        
        Args:
            broker: Message broker
            node_id: This node's ID
        """
        self.broker = broker
        self.node_id = node_id
        
        # Messaging
        self.transfer_producer = MessageProducer(broker, default_topic="state-transfer")
        self.transfer_consumer = MessageConsumer(
            broker=broker,
            topics=["state-transfer"],
            group_id=f"transfer-{node_id}"
        )
        
        # State
        self.state_providers = {}  # name -> (get_state_func, apply_state_func)
        self.transfer_requests = {}  # request_id -> (source_node, target_node, state_name)
        self.running = False
        self.transfer_task = None
    
    async def start(self):
        """Start state transfer manager."""
        if self.running:
            return
        
        self.running = True
        
        # Start consumer
        await self.transfer_consumer.start()
        self.transfer_consumer.on_message("state-transfer", self._handle_transfer_message)
        
        # Start transfer task
        self.transfer_task = asyncio.create_task(self._transfer_loop())
        
        logger.info(f"State transfer manager started for node {self.node_id}")
    
    async def stop(self):
        """Stop state transfer manager."""
        if not self.running:
            return
        
        self.running = False
        
        # Stop transfer task
        if self.transfer_task:
            self.transfer_task.cancel()
            try:
                await self.transfer_task
            except asyncio.CancelledError:
                pass
        
        # Stop consumer
        await self.transfer_consumer.stop()
        
        logger.info(f"State transfer manager stopped for node {self.node_id}")
    
    def register_state_provider(self, name: str, get_state_func, apply_state_func):
        """
        Register state provider.
        
        Args:
            name: State provider name
            get_state_func: Function to get state
            apply_state_func: Function to apply state
        """
        self.state_providers[name] = (get_state_func, apply_state_func)
        logger.info(f"Registered state provider {name}")
    
    def unregister_state_provider(self, name: str):
        """
        Unregister state provider.
        
        Args:
            name: State provider name
        """
        if name in self.state_providers:
            del self.state_providers[name]
            logger.info(f"Unregistered state provider {name}")
    
    async def request_state(self, target_node: str, state_name: str) -> str:
        """
        Request state from another node.
        
        Args:
            target_node: Target node ID
            state_name: State name
            
        Returns:
            Request ID
        """
        request_id = str(uuid.uuid4())
        
        message = {
            "type": "state_request",
            "request_id": request_id,
            "source_node": self.node_id,
            "target_node": target_node,
            "state_name": state_name,
            "timestamp": time.time()
        }
        
        # Store request
        self.transfer_requests[request_id] = (self.node_id, target_node, state_name)
        
        # Send request
        await self.transfer_producer.send(message, key=request_id)
        logger.info(f"Requested state {state_name} from node {target_node}")
        
        return request_id
    
    def _handle_transfer_message(self, message):
        """
        Handle transfer message.
        
        Args:
            message: Transfer message
        """
        try:
            data = message.value
            msg_type = data["type"]
            
            if msg_type == "state_request":
                self._handle_state_request(data)
            elif msg_type == "state_response":
                self._handle_state_response(data)
        except Exception as e:
            logger.error(f"Error handling transfer message: {e}")
    
    def _handle_state_request(self, data):
        """
        Handle state request.
        
        Args:
            data: State request data
        """
        request_id = data["request_id"]
        source_node = data["source_node"]
        state_name = data["state_name"]
        
        # Check if this node is the target
        if data["target_node"] != self.node_id:
            return
        
        # Check if state provider exists
        if state_name not in self.state_providers:
            logger.warning(f"State provider {state_name} not found")
            return
        
        # Get state
        get_state_func, _ = self.state_providers[state_name]
        
        # Send state response
        asyncio.create_task(self._send_state_response(request_id, source_node, state_name, get_state_func))
    
    async def _send_state_response(self, request_id: str, target_node: str, state_name: str, get_state_func):
        """
        Send state response.
        
        Args:
            request_id: Request ID
            target_node: Target node ID
            state_name: State name
            get_state_func: Function to get state
        """
        try:
            # Get state
            state = await get_state_func()
            
            message = {
                "type": "state_response",
                "request_id": request_id,
                "source_node": self.node_id,
                "target_node": target_node,
                "state_name": state_name,
                "state": state,
                "timestamp": time.time()
            }
            
            # Send response
            await self.transfer_producer.send(message, key=request_id)
            logger.info(f"Sent state {state_name} to node {target_node}")
        except Exception as e:
            logger.error(f"Error sending state response: {e}")
    
    def _handle_state_response(self, data):
        """
        Handle state response.
        
        Args:
            data: State response data
        """
        request_id = data["request_id"]
        source_node = data["source_node"]
        state_name = data["state_name"]
        state = data["state"]
        
        # Check if this node is the target
        if data["target_node"] != self.node_id:
            return
        
        # Check if request exists
        if request_id not in self.transfer_requests:
            logger.warning(f"State transfer request {request_id} not found")
            return
        
        # Check if state provider exists
        if state_name not in self.state_providers:
            logger.warning(f"State provider {state_name} not found")
            return
        
        # Apply state
        _, apply_state_func = self.state_providers[state_name]
        
        # Apply state
        asyncio.create_task(self._apply_state(state_name, state, apply_state_func))
        
        # Remove request
        del self.transfer_requests[request_id]
    
    async def _apply_state(self, state_name: str, state, apply_state_func):
        """
        Apply state.
        
        Args:
            state_name: State name
            state: State data
            apply_state_func: Function to apply state
        """
        try:
            await apply_state_func(state)
            logger.info(f"Applied state {state_name}")
        except Exception as e:
            logger.error(f"Error applying state {state_name}: {e}")
    
    async def _transfer_loop(self):
        """Main transfer loop."""
        while self.running:
            try:
                # Just keep the task alive
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in transfer loop: {e}")
                await asyncio.sleep(1.0)


class RecoveryCoordinator:
    """Coordinator for disaster recovery."""
    
    def __init__(self, broker: MessageBroker, node_id: str, 
                backup_manager: BackupManager, state_transfer_manager: StateTransferManager):
        """
        Initialize recovery coordinator.
        
        Args:
            broker: Message broker
            node_id: This node's ID
            backup_manager: Backup manager
            state_transfer_manager: State transfer manager
        """
        self.broker = broker
        self.node_id = node_id
        self.backup_manager = backup_manager
        self.state_transfer_manager = state_transfer_manager
        
        # Messaging
        self.recovery_producer = MessageProducer(broker, default_topic="recovery-coordination")
        self.recovery_consumer = MessageConsumer(
            broker=broker,
            topics=["recovery-coordination"],
            group_id=f"recovery-{node_id}"
        )
        
        # State
        self.recovery_handlers = {}  # name -> recovery_func
        self.running = False
        self.recovery_task = None
    
    async def start(self):
        """Start recovery coordinator."""
        if self.running:
            return
        
        self.running = True
        
        # Start consumer
        await self.recovery_consumer.start()
        self.recovery_consumer.on_message("recovery-coordination", self._handle_recovery_message)
        
        # Start recovery task
        self.recovery_task = asyncio.create_task(self._recovery_loop())
        
        logger.info(f"Recovery coordinator started for node {self.node_id}")
    
    async def stop(self):
        """Stop recovery coordinator."""
        if not self.running:
            return
        
        self.running = False
        
        # Stop recovery task
        if self.recovery_task:
            self.recovery_task.cancel()
            try:
                await self.recovery_task
            except asyncio.CancelledError:
                pass
        
        # Stop consumer
        await self.recovery_consumer.stop()
        
        logger.info(f"Recovery coordinator stopped for node {self.node_id}")
    
    def register_recovery_handler(self, name: str, recovery_func):
        """
        Register recovery handler.
        
        Args:
            name: Recovery handler name
            recovery_func: Function to handle recovery
        """
        self.recovery_handlers[name] = recovery_func
        logger.info(f"Registered recovery handler {name}")
    
    def unregister_recovery_handler(self, name: str):
        """
        Unregister recovery handler.
        
        Args:
            name: Recovery handler name
        """
        if name in self.recovery_handlers:
            del self.recovery_handlers[name]
            logger.info(f"Unregistered recovery handler {name}")
    
    async def initiate_recovery(self, recovery_type: str, params: Dict[str, Any]) -> str:
        """
        Initiate recovery process.
        
        Args:
            recovery_type: Type of recovery
            params: Recovery parameters
            
        Returns:
            Recovery ID
        """
        recovery_id = str(uuid.uuid4())
        
        message = {
            "type": "recovery_initiate",
            "recovery_id": recovery_id,
            "recovery_type": recovery_type,
            "params": params,
            "initiator_node": self.node_id,
            "timestamp": time.time()
        }
        
        # Send recovery initiation
        await self.recovery_producer.send(message, key=recovery_id)
        logger.info(f"Initiated recovery {recovery_id} of type {recovery_type}")
        
        return recovery_id
    
    async def report_recovery_status(self, recovery_id: str, status: str, details: Dict[str, Any] = None):
        """
        Report recovery status.
        
        Args:
            recovery_id: Recovery ID
            status: Recovery status
            details: Recovery details
        """
        message = {
            "type": "recovery_status",
            "recovery_id": recovery_id,
            "status": status,
            "details": details or {},
            "node_id": self.node_id,
            "timestamp": time.time()
        }
        
        # Send recovery status
        await self.recovery_producer.send(message, key=recovery_id)
        logger.info(f"Reported recovery status {status} for recovery {recovery_id}")
    
    def _handle_recovery_message(self, message):
        """
        Handle recovery message.
        
        Args:
            message: Recovery message
        """
        try:
            data = message.value
            msg_type = data["type"]
            
            if msg_type == "recovery_initiate":
                self._handle_recovery_initiate(data)
            elif msg_type == "recovery_status":
                self._handle_recovery_status(data)
        except Exception as e:
            logger.error(f"Error handling recovery message: {e}")
    
    def _handle_recovery_initiate(self, data):
        """
        Handle recovery initiation.
        
        Args:
            data: Recovery initiation data
        """
        recovery_id = data["recovery_id"]
        recovery_type = data["recovery_type"]
        params = data["params"]
        
        # Check if recovery handler exists
        if recovery_type not in self.recovery_handlers:
            logger.warning(f"Recovery handler {recovery_type} not found")
            return
        
        # Handle recovery
        recovery_func = self.recovery_handlers[recovery_type]
        
        # Execute recovery
        asyncio.create_task(self._execute_recovery(recovery_id, recovery_type, params, recovery_func))
    
    async def _execute_recovery(self, recovery_id: str, recovery_type: str, params: Dict[str, Any], recovery_func):
        """
        Execute recovery.
        
        Args:
            recovery_id: Recovery ID
            recovery_type: Recovery type
            params: Recovery parameters
            recovery_func: Recovery function
        """
        try:
            # Report starting
            await self.report_recovery_status(recovery_id, "starting")
            
            # Execute recovery
            result = await recovery_func(params)
            
            # Report completion
            await self.report_recovery_status(recovery_id, "completed", {"result": result})
            
            logger.info(f"Completed recovery {recovery_id} of type {recovery_type}")
        except Exception as e:
            logger.error(f"Error executing recovery {recovery_id}: {e}")
            
            # Report failure
            await self.report_recovery_status(recovery_id, "failed", {"error": str(e)})
    
    def _handle_recovery_status(self, data):
        """
        Handle recovery status.
        
        Args:
            data: Recovery status data
        """
        recovery_id = data["recovery_id"]
        status = data["status"]
        node_id = data["node_id"]
        
        logger.info(f"Received recovery status {status} for recovery {recovery_id} from node {node_id}")
    
    async def _recovery_loop(self):
        """Main recovery loop."""
        while self.running:
            try:
                # Just keep the task alive
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in recovery loop: {e}")
                await asyncio.sleep(1.0)
