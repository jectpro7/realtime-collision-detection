"""
Failover and throttling implementation for the distributed platform.
"""
import asyncio
import json
import time
import random
from typing import Dict, List, Optional, Set, Any, Tuple, Callable
import uuid

from ..common.models import NodeInfo, Task, TaskResult
from ..common.utils import get_logger, Timer, RateLimiter, CircuitBreaker
from ..messaging.messaging import MessageBroker, MessageConsumer, MessageProducer

logger = get_logger(__name__)

class FailoverManager:
    """Manager for handling node failover."""
    
    def __init__(self, broker: MessageBroker, node_id: str):
        """
        Initialize failover manager.
        
        Args:
            broker: Message broker
            node_id: This node's ID
        """
        self.broker = broker
        self.node_id = node_id
        
        # Messaging
        self.failover_producer = MessageProducer(broker, default_topic="failover-events")
        self.failover_consumer = MessageConsumer(
            broker=broker,
            topics=["failover-events"],
            group_id=f"failover-{node_id}"
        )
        
        # State
        self.node_assignments = {}  # resource_id -> node_id
        self.node_resources = {}  # node_id -> set(resource_id)
        self.resource_handlers = {}  # resource_type -> (takeover_func, release_func)
        self.failover_callbacks = []  # List of failover event callbacks
        
        # Tasks
        self.running = False
        self.failover_task = None
    
    async def start(self):
        """Start failover manager."""
        if self.running:
            return
        
        self.running = True
        
        # Start consumer
        await self.failover_consumer.start()
        self.failover_consumer.on_message("failover-events", self._handle_failover_message)
        
        # Start failover task
        self.failover_task = asyncio.create_task(self._failover_loop())
        
        logger.info(f"Failover manager started for node {self.node_id}")
    
    async def stop(self):
        """Stop failover manager."""
        if not self.running:
            return
        
        self.running = False
        
        # Stop failover task
        if self.failover_task:
            self.failover_task.cancel()
            try:
                await self.failover_task
            except asyncio.CancelledError:
                pass
        
        # Stop consumer
        await self.failover_consumer.stop()
        
        logger.info(f"Failover manager stopped for node {self.node_id}")
    
    def register_resource_handler(self, resource_type: str, takeover_func: Callable, release_func: Callable):
        """
        Register resource handler.
        
        Args:
            resource_type: Resource type
            takeover_func: Function to take over resource
            release_func: Function to release resource
        """
        self.resource_handlers[resource_type] = (takeover_func, release_func)
        logger.info(f"Registered resource handler for type {resource_type}")
    
    def unregister_resource_handler(self, resource_type: str):
        """
        Unregister resource handler.
        
        Args:
            resource_type: Resource type
        """
        if resource_type in self.resource_handlers:
            del self.resource_handlers[resource_type]
            logger.info(f"Unregistered resource handler for type {resource_type}")
    
    def on_failover_event(self, callback: Callable):
        """
        Register callback for failover events.
        
        Args:
            callback: Callback function(event_type, node_id, resource_id)
        """
        self.failover_callbacks.append(callback)
    
    async def assign_resource(self, resource_id: str, resource_type: str, node_id: str) -> bool:
        """
        Assign resource to node.
        
        Args:
            resource_id: Resource ID
            resource_type: Resource type
            node_id: Node ID
            
        Returns:
            True if assigned, False otherwise
        """
        # Check if already assigned to same node
        if resource_id in self.node_assignments and self.node_assignments[resource_id] == node_id:
            return True
        
        # Check if resource handler exists
        if resource_type not in self.resource_handlers:
            logger.warning(f"Resource handler for type {resource_type} not found")
            return False
        
        # Update assignments
        old_node = self.node_assignments.get(resource_id)
        self.node_assignments[resource_id] = node_id
        
        # Update node resources
        if node_id not in self.node_resources:
            self.node_resources[node_id] = set()
        self.node_resources[node_id].add(resource_id)
        
        # Remove from old node
        if old_node and old_node in self.node_resources:
            self.node_resources[old_node].discard(resource_id)
        
        # Announce assignment
        await self._announce_assignment(resource_id, resource_type, node_id, old_node)
        
        logger.info(f"Assigned resource {resource_id} to node {node_id}")
        return True
    
    async def release_resource(self, resource_id: str, resource_type: str) -> bool:
        """
        Release resource assignment.
        
        Args:
            resource_id: Resource ID
            resource_type: Resource type
            
        Returns:
            True if released, False otherwise
        """
        if resource_id not in self.node_assignments:
            return True
        
        # Check if resource handler exists
        if resource_type not in self.resource_handlers:
            logger.warning(f"Resource handler for type {resource_type} not found")
            return False
        
        # Get current node
        node_id = self.node_assignments[resource_id]
        
        # Update assignments
        del self.node_assignments[resource_id]
        
        # Update node resources
        if node_id in self.node_resources:
            self.node_resources[node_id].discard(resource_id)
        
        # Announce release
        await self._announce_release(resource_id, resource_type, node_id)
        
        logger.info(f"Released resource {resource_id} from node {node_id}")
        return True
    
    async def handle_node_failure(self, failed_node: str):
        """
        Handle node failure.
        
        Args:
            failed_node: Failed node ID
        """
        if failed_node not in self.node_resources:
            return
        
        # Get resources assigned to failed node
        resources = list(self.node_resources[failed_node])
        
        logger.info(f"Handling failure of node {failed_node} with {len(resources)} resources")
        
        # Reassign resources
        for resource_id in resources:
            # Find resource type
            resource_type = None
            for r_type, (_, _) in self.resource_handlers.items():
                if resource_id.startswith(f"{r_type}-"):
                    resource_type = r_type
                    break
            
            if not resource_type:
                logger.warning(f"Could not determine type for resource {resource_id}")
                continue
            
            # Find new node (simple round-robin for now)
            new_node = self._select_failover_node(failed_node)
            
            if not new_node:
                logger.warning(f"No available node for failover of resource {resource_id}")
                continue
            
            # Reassign
            await self.assign_resource(resource_id, resource_type, new_node)
    
    def _select_failover_node(self, failed_node: str) -> Optional[str]:
        """
        Select node for failover.
        
        Args:
            failed_node: Failed node ID
            
        Returns:
            Selected node ID or None if no suitable node
        """
        # Simple implementation: select random node that is not the failed node
        available_nodes = [node for node in self.node_resources.keys() if node != failed_node]
        
        if not available_nodes:
            return None
        
        return random.choice(available_nodes)
    
    async def _announce_assignment(self, resource_id: str, resource_type: str, node_id: str, old_node: Optional[str]):
        """
        Announce resource assignment.
        
        Args:
            resource_id: Resource ID
            resource_type: Resource type
            node_id: Node ID
            old_node: Old node ID
        """
        message = {
            "type": "resource_assignment",
            "resource_id": resource_id,
            "resource_type": resource_type,
            "node_id": node_id,
            "old_node": old_node,
            "timestamp": time.time()
        }
        
        await self.failover_producer.send(message, key=resource_id)
        
        # Notify callbacks
        for callback in self.failover_callbacks:
            try:
                callback("assignment", node_id, resource_id)
            except Exception as e:
                logger.error(f"Error in failover callback: {e}")
    
    async def _announce_release(self, resource_id: str, resource_type: str, node_id: str):
        """
        Announce resource release.
        
        Args:
            resource_id: Resource ID
            resource_type: Resource type
            node_id: Node ID
        """
        message = {
            "type": "resource_release",
            "resource_id": resource_id,
            "resource_type": resource_type,
            "node_id": node_id,
            "timestamp": time.time()
        }
        
        await self.failover_producer.send(message, key=resource_id)
        
        # Notify callbacks
        for callback in self.failover_callbacks:
            try:
                callback("release", node_id, resource_id)
            except Exception as e:
                logger.error(f"Error in failover callback: {e}")
    
    def _handle_failover_message(self, message):
        """
        Handle failover message.
        
        Args:
            message: Failover message
        """
        try:
            data = message.value
            msg_type = data["type"]
            
            if msg_type == "resource_assignment":
                self._handle_resource_assignment(data)
            elif msg_type == "resource_release":
                self._handle_resource_release(data)
            elif msg_type == "node_failure":
                self._handle_node_failure_message(data)
        except Exception as e:
            logger.error(f"Error handling failover message: {e}")
    
    def _handle_resource_assignment(self, data):
        """
        Handle resource assignment message.
        
        Args:
            data: Resource assignment data
        """
        resource_id = data["resource_id"]
        resource_type = data["resource_type"]
        node_id = data["node_id"]
        old_node = data["old_node"]
        
        # Update local state
        self.node_assignments[resource_id] = node_id
        
        if node_id not in self.node_resources:
            self.node_resources[node_id] = set()
        self.node_resources[node_id].add(resource_id)
        
        if old_node and old_node in self.node_resources:
            self.node_resources[old_node].discard(resource_id)
        
        # If this node is taking over, call takeover function
        if node_id == self.node_id and resource_type in self.resource_handlers:
            takeover_func, _ = self.resource_handlers[resource_type]
            asyncio.create_task(self._execute_takeover(resource_id, takeover_func))
        
        # If this node is releasing, call release function
        if old_node == self.node_id and resource_type in self.resource_handlers:
            _, release_func = self.resource_handlers[resource_type]
            asyncio.create_task(self._execute_release(resource_id, release_func))
    
    def _handle_resource_release(self, data):
        """
        Handle resource release message.
        
        Args:
            data: Resource release data
        """
        resource_id = data["resource_id"]
        resource_type = data["resource_type"]
        node_id = data["node_id"]
        
        # Update local state
        if resource_id in self.node_assignments:
            del self.node_assignments[resource_id]
        
        if node_id in self.node_resources:
            self.node_resources[node_id].discard(resource_id)
        
        # If this node is releasing, call release function
        if node_id == self.node_id and resource_type in self.resource_handlers:
            _, release_func = self.resource_handlers[resource_type]
            asyncio.create_task(self._execute_release(resource_id, release_func))
    
    def _handle_node_failure_message(self, data):
        """
        Handle node failure message.
        
        Args:
            data: Node failure data
        """
        failed_node = data["node_id"]
        
        # Handle node failure
        asyncio.create_task(self.handle_node_failure(failed_node))
    
    async def _execute_takeover(self, resource_id: str, takeover_func: Callable):
        """
        Execute resource takeover.
        
        Args:
            resource_id: Resource ID
            takeover_func: Takeover function
        """
        try:
            await takeover_func(resource_id)
            logger.info(f"Took over resource {resource_id}")
        except Exception as e:
            logger.error(f"Error taking over resource {resource_id}: {e}")
    
    async def _execute_release(self, resource_id: str, release_func: Callable):
        """
        Execute resource release.
        
        Args:
            resource_id: Resource ID
            release_func: Release function
        """
        try:
            await release_func(resource_id)
            logger.info(f"Released resource {resource_id}")
        except Exception as e:
            logger.error(f"Error releasing resource {resource_id}: {e}")
    
    async def _failover_loop(self):
        """Main failover loop."""
        while self.running:
            try:
                # Just keep the task alive
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in failover loop: {e}")
                await asyncio.sleep(1.0)


class ThrottlingManager:
    """Manager for request throttling and rate limiting."""
    
    def __init__(self):
        """Initialize throttling manager."""
        # Rate limiters
        self.global_limiter = RateLimiter(rate=10000, capacity=10000)  # 10K requests/sec
        self.endpoint_limiters = {}  # endpoint -> RateLimiter
        self.client_limiters = {}  # client_id -> RateLimiter
        
        # Circuit breakers
        self.endpoint_breakers = {}  # endpoint -> CircuitBreaker
        
        # Throttling policies
        self.policies = {}  # policy_name -> ThrottlingPolicy
        
        logger.info("Throttling manager initialized")
    
    def create_rate_limiter(self, name: str, rate: float, capacity: float) -> RateLimiter:
        """
        Create rate limiter.
        
        Args:
            name: Rate limiter name
            rate: Tokens per second
            capacity: Maximum number of tokens
            
        Returns:
            Rate limiter instance
        """
        limiter = RateLimiter(rate=rate, capacity=capacity)
        logger.info(f"Created rate limiter {name} with rate {rate}/sec and capacity {capacity}")
        return limiter
    
    def create_circuit_breaker(self, name: str, failure_threshold: int = 5,
                              recovery_timeout: float = 30.0,
                              half_open_max_calls: int = 3) -> CircuitBreaker:
        """
        Create circuit breaker.
        
        Args:
            name: Circuit breaker name
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Time in seconds before attempting recovery
            half_open_max_calls: Maximum calls in half-open state
            
        Returns:
            Circuit breaker instance
        """
        breaker = CircuitBreaker(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            half_open_max_calls=half_open_max_calls
        )
        logger.info(f"Created circuit breaker {name}")
        return breaker
    
    def register_endpoint_limiter(self, endpoint: str, rate: float, capacity: float):
        """
        Register rate limiter for endpoint.
        
        Args:
            endpoint: Endpoint name
            rate: Tokens per second
            capacity: Maximum number of tokens
        """
        self.endpoint_limiters[endpoint] = self.create_rate_limiter(
            f"endpoint-{endpoint}", rate, capacity
        )
    
    def register_client_limiter(self, client_id: str, rate: float, capacity: float):
        """
        Register rate limiter for client.
        
        Args:
            client_id: Client ID
            rate: Tokens per second
            capacity: Maximum number of tokens
        """
        self.client_limiters[client_id] = self.create_rate_limiter(
            f"client-{client_id}", rate, capacity
        )
    
    def register_endpoint_breaker(self, endpoint: str, failure_threshold: int = 5,
                                 recovery_timeout: float = 30.0,
                                 half_open_max_calls: int = 3):
        """
        Register circuit breaker for endpoint.
        
        Args:
            endpoint: Endpoint name
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Time in seconds before attempting recovery
            half_open_max_calls: Maximum calls in half-open state
        """
        self.endpoint_breakers[endpoint] = self.create_circuit_breaker(
            f"endpoint-{endpoint}",
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            half_open_max_calls=half_open_max_calls
        )
    
    def create_throttling_policy(self, name: str, rules: List[Dict[str, Any]]):
        """
        Create throttling policy.
        
        Args:
            name: Policy name
            rules: List of throttling rules
        """
        policy = ThrottlingPolicy(name, rules)
        self.policies[name] = policy
        logger.info(f"Created throttling policy {name} with {len(rules)} rules")
    
    def allow_request(self, endpoint: str, client_id: Optional[str] = None) -> bool:
        """
        Check if request should be allowed.
        
        Args:
            endpoint: Endpoint name
            client_id: Client ID (optional)
            
        Returns:
            True if request is allowed, False otherwise
        """
        # Check global rate limiter
        if not self.global_limiter.acquire():
            logger.warning(f"Global rate limit exceeded for endpoint {endpoint}")
            return False
        
        # Check endpoint rate limiter
        if endpoint in self.endpoint_limiters:
            if not self.endpoint_limiters[endpoint].acquire():
                logger.warning(f"Endpoint rate limit exceeded for {endpoint}")
                return False
        
        # Check client rate limiter
        if client_id and client_id in self.client_limiters:
            if not self.client_limiters[client_id].acquire():
                logger.warning(f"Client rate limit exceeded for {client_id}")
                return False
        
        # Check endpoint circuit breaker
        if endpoint in self.endpoint_breakers:
            if not self.endpoint_breakers[endpoint].allow_request():
                logger.warning(f"Circuit open for endpoint {endpoint}")
                return False
        
        return True
    
    def record_success(self, endpoint: str):
        """
        Record successful request.
        
        Args:
            endpoint: Endpoint name
        """
        if endpoint in self.endpoint_breakers:
            self.endpoint_breakers[endpoint].record_success()
    
    def record_failure(self, endpoint: str):
        """
        Record failed request.
        
        Args:
            endpoint: Endpoint name
        """
        if endpoint in self.endpoint_breakers:
            self.endpoint_breakers[endpoint].record_failure()
    
    def apply_policy(self, policy_name: str, context: Dict[str, Any]) -> bool:
        """
        Apply throttling policy.
        
        Args:
            policy_name: Policy name
            context: Request context
            
        Returns:
            True if request is allowed, False otherwise
        """
        if policy_name not in self.policies:
            logger.warning(f"Throttling policy {policy_name} not found")
            return True
        
        return self.policies[policy_name].apply(context)


class ThrottlingPolicy:
    """Policy for request throttling."""
    
    def __init__(self, name: str, rules: List[Dict[str, Any]]):
        """
        Initialize throttling policy.
        
        Args:
            name: Policy name
            rules: List of throttling rules
        """
        self.name = name
        self.rules = rules
        
        # Compile rules
        self.compiled_rules = []
        for rule in rules:
            self.compiled_rules.append(self._compile_rule(rule))
    
    def _compile_rule(self, rule: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compile throttling rule.
        
        Args:
            rule: Throttling rule
            
        Returns:
            Compiled rule
        """
        compiled = rule.copy()
        
        # Create rate limiter if needed
        if "rate" in rule and "capacity" in rule:
            compiled["limiter"] = RateLimiter(
                rate=rule["rate"],
                capacity=rule["capacity"]
            )
        
        return compiled
    
    def apply(self, context: Dict[str, Any]) -> bool:
        """
        Apply throttling policy.
        
        Args:
            context: Request context
            
        Returns:
            True if request is allowed, False otherwise
        """
        for rule in self.compiled_rules:
            # Check conditions
            if "conditions" in rule:
                match = True
                for key, value in rule["conditions"].items():
                    if key not in context or context[key] != value:
                        match = False
                        break
                
                if not match:
                    continue
            
            # Apply rate limiting
            if "limiter" in rule:
                if not rule["limiter"].acquire():
                    logger.warning(f"Rate limit exceeded for policy {self.name}")
                    return False
            
            # Apply priority
            if "priority" in rule and "current_priority" in context:
                if context["current_priority"] < rule["priority"]:
                    logger.warning(f"Request priority {context['current_priority']} below threshold {rule['priority']}")
                    return False
        
        return True


class AdaptiveThrottling:
    """Adaptive throttling based on system load."""
    
    def __init__(self, throttling_manager: ThrottlingManager,
                update_interval: float = 10.0):
        """
        Initialize adaptive throttling.
        
        Args:
            throttling_manager: Throttling manager
            update_interval: Update interval in seconds
        """
        self.throttling_manager = throttling_manager
        self.update_interval = update_interval
        
        # State
        self.load_metrics = {}  # endpoint -> LoadMetrics
        self.running = False
        self.update_task = None
    
    async def start(self):
        """Start adaptive throttling."""
        if self.running:
            return
        
        self.running = True
        
        # Start update task
        self.update_task = asyncio.create_task(self._update_loop())
        
        logger.info("Adaptive throttling started")
    
    async def stop(self):
        """Stop adaptive throttling."""
        if not self.running:
            return
        
        self.running = False
        
        # Stop update task
        if self.update_task:
            self.update_task.cancel()
            try:
                await self.update_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Adaptive throttling stopped")
    
    def update_load_metrics(self, endpoint: str, cpu_usage: float, memory_usage: float,
                           queue_size: int, processing_rate: float, average_latency: float):
        """
        Update load metrics.
        
        Args:
            endpoint: Endpoint name
            cpu_usage: CPU usage (0.0-1.0)
            memory_usage: Memory usage (0.0-1.0)
            queue_size: Queue size
            processing_rate: Processing rate (requests/sec)
            average_latency: Average latency (ms)
        """
        self.load_metrics[endpoint] = {
            "cpu_usage": cpu_usage,
            "memory_usage": memory_usage,
            "queue_size": queue_size,
            "processing_rate": processing_rate,
            "average_latency": average_latency,
            "timestamp": time.time()
        }
    
    async def _update_loop(self):
        """Main update loop."""
        while self.running:
            try:
                # Update rate limiters based on load
                self._adjust_rate_limiters()
                
                # Sleep until next update
                await asyncio.sleep(self.update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in update loop: {e}")
                await asyncio.sleep(1.0)
    
    def _adjust_rate_limiters(self):
        """Adjust rate limiters based on load metrics."""
        for endpoint, metrics in self.load_metrics.items():
            if endpoint not in self.throttling_manager.endpoint_limiters:
                continue
            
            limiter = self.throttling_manager.endpoint_limiters[endpoint]
            
            # Simple adaptive algorithm
            cpu_usage = metrics["cpu_usage"]
            
            # Reduce rate if CPU usage is high
            if cpu_usage > 0.8:
                new_rate = limiter.rate * 0.8  # Reduce by 20%
                limiter.rate = max(10, new_rate)  # Minimum rate
                logger.info(f"Reduced rate for {endpoint} to {limiter.rate:.1f}/sec due to high CPU usage")
            
            # Increase rate if CPU usage is low
            elif cpu_usage < 0.5:
                new_rate = limiter.rate * 1.1  # Increase by 10%
                limiter.rate = min(10000, new_rate)  # Maximum rate
                logger.info(f"Increased rate for {endpoint} to {limiter.rate:.1f}/sec due to low CPU usage")
