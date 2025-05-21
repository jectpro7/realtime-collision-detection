"""
Scheduler implementation for the distributed platform.
"""
import asyncio
import time
from typing import Dict, List, Optional, Set, Any
import uuid

from ..common.models import Task, TaskResult, NodeInfo, LoadMetrics
from ..common.utils import get_logger, Timer
from ..messaging.messaging import MessageBroker, MessageConsumer, TaskProducer

logger = get_logger(__name__)

class Scheduler:
    """Task scheduler for the distributed platform."""
    
    def __init__(self, broker: MessageBroker):
        """
        Initialize scheduler.
        
        Args:
            broker: Message broker
        """
        self.broker = broker
        self.nodes: Dict[str, NodeInfo] = {}
        self.grid_nodes: Dict[str, List[str]] = {}  # grid_id -> [node_id]
        self.node_load: Dict[str, LoadMetrics] = {}
        
        # Task tracking
        self.pending_tasks: Dict[str, Task] = {}
        self.task_assignments: Dict[str, str] = {}  # task_id -> node_id
        
        # Messaging
        self.task_producer = TaskProducer(broker)
        self.result_consumer = MessageConsumer(
            broker=broker,
            topics=["task-results"],
            group_id="scheduler"
        )
        
        # State
        self.running = False
        self.scheduling_task = None
        self.cleanup_task = None
    
    async def start(self):
        """Start the scheduler."""
        if self.running:
            return
        
        self.running = True
        
        # Start result consumer
        await self.result_consumer.start()
        self.result_consumer.on_message("task-results", self._handle_result)
        
        # Start scheduling task
        self.scheduling_task = asyncio.create_task(self._scheduling_loop())
        
        # Start cleanup task
        self.cleanup_task = asyncio.create_task(self._cleanup_loop())
        
        logger.info("Scheduler started")
    
    async def stop(self):
        """Stop the scheduler."""
        if not self.running:
            return
        
        self.running = False
        
        # Stop tasks
        if self.scheduling_task:
            self.scheduling_task.cancel()
            try:
                await self.scheduling_task
            except asyncio.CancelledError:
                pass
        
        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
        
        # Stop result consumer
        await self.result_consumer.stop()
        
        logger.info("Scheduler stopped")
    
    def register_node(self, node_info: NodeInfo):
        """
        Register a compute node.
        
        Args:
            node_info: Node information
        """
        self.nodes[node_info.node_id] = node_info
        
        # Update grid -> nodes mapping
        for grid_id in node_info.grid_ids:
            if grid_id not in self.grid_nodes:
                self.grid_nodes[grid_id] = []
            if node_info.node_id not in self.grid_nodes[grid_id]:
                self.grid_nodes[grid_id].append(node_info.node_id)
        
        logger.info(f"Registered node {node_info.node_id} for grids {node_info.grid_ids}")
    
    def unregister_node(self, node_id: str):
        """
        Unregister a compute node.
        
        Args:
            node_id: Node ID
        """
        if node_id not in self.nodes:
            return
        
        node_info = self.nodes[node_id]
        
        # Update grid -> nodes mapping
        for grid_id in node_info.grid_ids:
            if grid_id in self.grid_nodes and node_id in self.grid_nodes[grid_id]:
                self.grid_nodes[grid_id].remove(node_id)
                if not self.grid_nodes[grid_id]:
                    del self.grid_nodes[grid_id]
        
        # Remove node
        del self.nodes[node_id]
        if node_id in self.node_load:
            del self.node_load[node_id]
        
        logger.info(f"Unregistered node {node_id}")
    
    def update_node_load(self, node_id: str, load_metrics: LoadMetrics):
        """
        Update load metrics for a node.
        
        Args:
            node_id: Node ID
            load_metrics: Load metrics
        """
        if node_id in self.nodes:
            self.node_load[node_id] = load_metrics
            self.nodes[node_id].load = load_metrics.cpu_usage
    
    async def submit_task(self, task: Task) -> bool:
        """
        Submit a task for execution.
        
        Args:
            task: Task to execute
            
        Returns:
            True if task was submitted, False otherwise
        """
        # Store task
        self.pending_tasks[task.task_id] = task
        
        # Schedule task immediately if possible
        node_id = self._select_node_for_task(task)
        if node_id:
            return await self._assign_task_to_node(task, node_id)
        
        # Task will be scheduled in scheduling loop
        return True
    
    def _select_node_for_task(self, task: Task) -> Optional[str]:
        """
        Select best node for task.
        
        Args:
            task: Task to schedule
            
        Returns:
            Node ID or None if no suitable node found
        """
        # For collision detection tasks, select node responsible for grid
        if task.task_type == "collision_detection" and "grid_id" in task.data:
            grid_id = task.data["grid_id"]
            return self._select_node_for_grid(grid_id)
        
        # For other tasks, select least loaded node
        return self._select_least_loaded_node()
    
    def _select_node_for_grid(self, grid_id: str) -> Optional[str]:
        """
        Select node for grid.
        
        Args:
            grid_id: Grid ID
            
        Returns:
            Node ID or None if no node for grid
        """
        if grid_id not in self.grid_nodes or not self.grid_nodes[grid_id]:
            return None
        
        # Select least loaded node for grid
        nodes = self.grid_nodes[grid_id]
        best_node = None
        best_load = float('inf')
        
        for node_id in nodes:
            if node_id in self.nodes and self.nodes[node_id].status == "active":
                load = self.nodes[node_id].load
                if load < best_load:
                    best_node = node_id
                    best_load = load
        
        return best_node
    
    def _select_least_loaded_node(self) -> Optional[str]:
        """
        Select least loaded node.
        
        Returns:
            Node ID or None if no active nodes
        """
        best_node = None
        best_load = float('inf')
        
        for node_id, node_info in self.nodes.items():
            if node_info.status == "active":
                load = node_info.load
                if load < best_load:
                    best_node = node_id
                    best_load = load
        
        return best_node
    
    async def _assign_task_to_node(self, task: Task, node_id: str) -> bool:
        """
        Assign task to node.
        
        Args:
            task: Task to assign
            node_id: Node ID
            
        Returns:
            True if task was assigned, False otherwise
        """
        # Send task to node
        success = await self.task_producer.send_task(task)
        
        if success:
            # Record assignment
            self.task_assignments[task.task_id] = node_id
            logger.debug(f"Assigned task {task.task_id} to node {node_id}")
            return True
        
        logger.warning(f"Failed to assign task {task.task_id} to node {node_id}")
        return False
    
    def _handle_result(self, message):
        """
        Handle task result message.
        
        Args:
            message: Task result message
        """
        try:
            data = message.value
            result = TaskResult(**data)
            
            # Remove task from pending and assignment
            task_id = result.task_id
            if task_id in self.pending_tasks:
                del self.pending_tasks[task_id]
            if task_id in self.task_assignments:
                del self.task_assignments[task_id]
            
            logger.debug(f"Received result for task {task_id}: success={result.success}")
        except Exception as e:
            logger.error(f"Error handling task result: {e}")
    
    async def _scheduling_loop(self):
        """Main scheduling loop."""
        while self.running:
            try:
                # Schedule pending tasks
                await self._schedule_pending_tasks()
                
                # Balance load if needed
                await self._balance_load()
                
                # Sleep
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in scheduling loop: {e}")
                await asyncio.sleep(1.0)
    
    async def _schedule_pending_tasks(self):
        """Schedule pending tasks."""
        # Sort tasks by priority (higher first) and creation time
        sorted_tasks = sorted(
            self.pending_tasks.values(),
            key=lambda t: (-t.priority, t.created_at)
        )
        
        for task in sorted_tasks:
            # Skip if already assigned
            if task.task_id in self.task_assignments:
                continue
            
            # Select node
            node_id = self._select_node_for_task(task)
            if not node_id:
                continue
            
            # Assign task
            await self._assign_task_to_node(task, node_id)
    
    async def _balance_load(self):
        """Balance load across nodes."""
        # This is a simplified implementation
        # In a real system, would implement more sophisticated load balancing
        pass
    
    async def _cleanup_loop(self):
        """Cleanup loop for timed out tasks."""
        while self.running:
            try:
                now = time.time()
                
                # Find timed out tasks
                timed_out = []
                for task_id, task in self.pending_tasks.items():
                    if now - task.created_at > task.timeout:
                        timed_out.append(task_id)
                
                # Remove timed out tasks
                for task_id in timed_out:
                    if task_id in self.pending_tasks:
                        logger.warning(f"Task {task_id} timed out")
                        del self.pending_tasks[task_id]
                    if task_id in self.task_assignments:
                        del self.task_assignments[task_id]
                
                # Sleep
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
                await asyncio.sleep(1.0)


class SchedulerClient:
    """Client for interacting with the scheduler."""
    
    def __init__(self, broker: MessageBroker):
        """
        Initialize scheduler client.
        
        Args:
            broker: Message broker
        """
        self.broker = broker
        self.task_producer = TaskProducer(broker)
        self.result_consumer = MessageConsumer(
            broker=broker,
            topics=["task-results"],
            group_id=f"client-{str(uuid.uuid4())[:8]}"
        )
        self.result_callbacks: Dict[str, Any] = {}
        self.running = False
    
    async def start(self):
        """Start the client."""
        if self.running:
            return
        
        self.running = True
        
        # Start result consumer
        await self.result_consumer.start()
        self.result_consumer.on_message("task-results", self._handle_result)
    
    async def stop(self):
        """Stop the client."""
        if not self.running:
            return
        
        self.running = False
        
        # Stop result consumer
        await self.result_consumer.stop()
    
    async def submit_task(self, task: Task, callback=None) -> bool:
        """
        Submit a task for execution.
        
        Args:
            task: Task to execute
            callback: Optional callback for result
            
        Returns:
            True if task was submitted, False otherwise
        """
        if callback:
            self.result_callbacks[task.task_id] = callback
        
        return await self.task_producer.send_task(task)
    
    def _handle_result(self, message):
        """
        Handle task result message.
        
        Args:
            message: Task result message
        """
        try:
            data = message.value
            result = TaskResult(**data)
            
            # Call callback if registered
            task_id = result.task_id
            if task_id in self.result_callbacks:
                callback = self.result_callbacks[task_id]
                try:
                    callback(result)
                except Exception as e:
                    logger.error(f"Error in result callback: {e}")
                finally:
                    del self.result_callbacks[task_id]
        except Exception as e:
            logger.error(f"Error handling task result: {e}")
