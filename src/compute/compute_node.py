"""
Compute node implementation for the distributed platform.
"""
import asyncio
import time
from typing import Dict, List, Optional, Set, Tuple, Any
import uuid

from ..common.models import (
    LocationData, Position, Vector, NodeConfig, NodeInfo, 
    CollisionRisk, Task, TaskResult
)
from ..common.utils import get_logger, Timer, CircuitBreaker
from ..messaging.messaging import (
    MessageBroker, MessageConsumer, TaskResultProducer
)

logger = get_logger(__name__)

class SpatialIndex:
    """Spatial index for efficient proximity queries."""
    
    def __init__(self, cell_size: float = 10.0):
        """
        Initialize spatial index.
        
        Args:
            cell_size: Size of grid cells
        """
        self.cell_size = cell_size
        self.grid: Dict[Tuple[int, int, int], Set[str]] = {}
        self.positions: Dict[str, Position] = {}
    
    def _get_cell(self, position: Position) -> Tuple[int, int, int]:
        """Get grid cell for position."""
        x = int(position.x / self.cell_size)
        y = int(position.y / self.cell_size)
        z = int(position.z / self.cell_size)
        return (x, y, z)
    
    def _get_nearby_cells(self, position: Position, radius: float) -> List[Tuple[int, int, int]]:
        """Get grid cells within radius of position."""
        center_cell = self._get_cell(position)
        cells = []
        
        # Calculate cell range
        cell_radius = int(radius / self.cell_size) + 1
        for x in range(center_cell[0] - cell_radius, center_cell[0] + cell_radius + 1):
            for y in range(center_cell[1] - cell_radius, center_cell[1] + cell_radius + 1):
                for z in range(center_cell[2] - cell_radius, center_cell[2] + cell_radius + 1):
                    cells.append((x, y, z))
        
        return cells
    
    def insert(self, vehicle_id: str, position: Position):
        """
        Insert vehicle into spatial index.
        
        Args:
            vehicle_id: Vehicle ID
            position: Vehicle position
        """
        # Remove from old position if exists
        self.remove(vehicle_id)
        
        # Insert into new position
        cell = self._get_cell(position)
        if cell not in self.grid:
            self.grid[cell] = set()
        
        self.grid[cell].add(vehicle_id)
        self.positions[vehicle_id] = position
    
    def remove(self, vehicle_id: str) -> bool:
        """
        Remove vehicle from spatial index.
        
        Args:
            vehicle_id: Vehicle ID
            
        Returns:
            True if vehicle was removed, False if not found
        """
        if vehicle_id not in self.positions:
            return False
        
        position = self.positions[vehicle_id]
        cell = self._get_cell(position)
        
        if cell in self.grid and vehicle_id in self.grid[cell]:
            self.grid[cell].remove(vehicle_id)
            if not self.grid[cell]:
                del self.grid[cell]
        
        del self.positions[vehicle_id]
        return True
    
    def query_nearby(self, position: Position, radius: float) -> List[str]:
        """
        Query vehicles within radius of position.
        
        Args:
            position: Query position
            radius: Query radius
            
        Returns:
            List of vehicle IDs within radius
        """
        cells = self._get_nearby_cells(position, radius)
        nearby_vehicles = []
        
        for cell in cells:
            if cell in self.grid:
                for vehicle_id in self.grid[cell]:
                    vehicle_pos = self.positions[vehicle_id]
                    if position.distance_to(vehicle_pos) <= radius:
                        nearby_vehicles.append(vehicle_id)
        
        return nearby_vehicles
    
    def get_position(self, vehicle_id: str) -> Optional[Position]:
        """
        Get position of vehicle.
        
        Args:
            vehicle_id: Vehicle ID
            
        Returns:
            Vehicle position or None if not found
        """
        return self.positions.get(vehicle_id)
    
    def get_all_vehicles(self) -> List[str]:
        """
        Get all vehicle IDs in the index.
        
        Returns:
            List of all vehicle IDs
        """
        return list(self.positions.keys())
    
    def get_vehicle_count(self) -> int:
        """
        Get number of vehicles in the index.
        
        Returns:
            Number of vehicles
        """
        return len(self.positions)


class VehicleState:
    """State of a vehicle including position history."""
    
    def __init__(self, vehicle_id: str, max_history: int = 10):
        """
        Initialize vehicle state.
        
        Args:
            vehicle_id: Vehicle ID
            max_history: Maximum number of historical positions to keep
        """
        self.vehicle_id = vehicle_id
        self.max_history = max_history
        self.history: List[LocationData] = []
        self.last_update = 0
    
    def update(self, location_data: LocationData):
        """
        Update vehicle state with new location data.
        
        Args:
            location_data: New location data
        """
        self.history.append(location_data)
        if len(self.history) > self.max_history:
            self.history.pop(0)
        
        self.last_update = time.time()
    
    def get_current_location(self) -> Optional[LocationData]:
        """
        Get current location of vehicle.
        
        Returns:
            Current location or None if no history
        """
        if not self.history:
            return None
        return self.history[-1]
    
    def predict_position(self, time_delta: float) -> Optional[Position]:
        """
        Predict position after time_delta seconds.
        
        Args:
            time_delta: Time in seconds
            
        Returns:
            Predicted position or None if prediction not possible
        """
        if len(self.history) < 2:
            return None
        
        current = self.history[-1]
        
        # Simple linear prediction based on current velocity
        return Position(
            x=current.position.x + current.velocity.x * time_delta,
            y=current.position.y + current.velocity.y * time_delta,
            z=current.position.z + current.velocity.z * time_delta
        )


class CollisionDetector:
    """Collision detection algorithm implementation."""
    
    def __init__(self, prediction_time: float = 5.0, risk_threshold: float = 0.5):
        """
        Initialize collision detector.
        
        Args:
            prediction_time: Time in seconds to predict ahead
            risk_threshold: Threshold for collision risk (0.0-1.0)
        """
        self.prediction_time = prediction_time
        self.risk_threshold = risk_threshold
    
    def detect_collisions(self, vehicle: VehicleState, 
                         nearby_vehicles: Dict[str, VehicleState]) -> List[CollisionRisk]:
        """
        Detect potential collisions between vehicle and nearby vehicles.
        
        Args:
            vehicle: Vehicle state
            nearby_vehicles: Dictionary of nearby vehicle states
            
        Returns:
            List of collision risks
        """
        risks = []
        
        vehicle_loc = vehicle.get_current_location()
        if not vehicle_loc:
            return risks
        
        # Vehicle dimensions (simplified as sphere)
        vehicle_radius = 2.0  # meters
        
        for other_id, other_vehicle in nearby_vehicles.items():
            if other_id == vehicle.vehicle_id:
                continue
            
            other_loc = other_vehicle.get_current_location()
            if not other_loc:
                continue
            
            # Current distance
            current_distance = vehicle_loc.position.distance_to(other_loc.position)
            
            # Skip if already too far
            if current_distance > 50.0:  # meters
                continue
            
            # Predict positions
            vehicle_future = vehicle.predict_position(self.prediction_time)
            other_future = other_vehicle.predict_position(self.prediction_time)
            
            if not vehicle_future or not other_future:
                continue
            
            # Future distance
            future_distance = vehicle_future.distance_to(other_future)
            
            # Calculate relative velocity
            rel_velocity = Vector(
                x=vehicle_loc.velocity.x - other_loc.velocity.x,
                y=vehicle_loc.velocity.y - other_loc.velocity.y,
                z=vehicle_loc.velocity.z - other_loc.velocity.z
            )
            rel_speed = rel_velocity.magnitude()
            
            # Skip if moving away from each other
            if future_distance > current_distance and current_distance > vehicle_radius * 2:
                continue
            
            # Calculate collision risk
            min_distance = max(0.1, future_distance)  # Avoid division by zero
            risk_level = min(1.0, (vehicle_radius * 2) / min_distance * rel_speed / 10.0)
            
            # Skip if risk below threshold
            if risk_level < self.risk_threshold:
                continue
            
            # Calculate estimated collision time
            time_to_collision = self.prediction_time
            if future_distance < vehicle_radius * 2:
                # Interpolate collision time
                if current_distance > future_distance:
                    collision_ratio = (current_distance - vehicle_radius * 2) / (current_distance - future_distance)
                    time_to_collision = max(0.1, self.prediction_time * collision_ratio)
            
            # Create collision risk
            collision_position = Position(
                x=(vehicle_future.x + other_future.x) / 2,
                y=(vehicle_future.y + other_future.y) / 2,
                z=(vehicle_future.z + other_future.z) / 2
            )
            
            risk = CollisionRisk.create(
                vehicle_id1=vehicle.vehicle_id,
                vehicle_id2=other_id,
                risk_level=risk_level,
                estimated_collision_time=time.time() + time_to_collision,
                position=collision_position,
                relative_velocity=rel_speed
            )
            
            risks.append(risk)
        
        return risks


class ComputeNode:
    """Compute node for processing vehicle location data and detecting collisions."""
    
    def __init__(self, node_id: str, grid_id: str, broker: MessageBroker, config: NodeConfig):
        """
        Initialize compute node.
        
        Args:
            node_id: Node ID
            grid_id: Grid ID this node is responsible for
            broker: Message broker
            config: Node configuration
        """
        self.node_id = node_id
        self.grid_id = grid_id
        self.broker = broker
        self.config = config
        
        # Components
        self.spatial_index = SpatialIndex(cell_size=10.0)
        self.vehicle_states: Dict[str, VehicleState] = {}
        self.collision_detector = CollisionDetector()
        
        # Messaging
        self.location_consumer = MessageConsumer(
            broker=broker,
            topics=["vehicle-locations"],
            group_id=f"compute-{node_id}"
        )
        self.task_consumer = MessageConsumer(
            broker=broker,
            topics=["computation-tasks"],
            group_id=f"compute-{node_id}"
        )
        self.result_producer = TaskResultProducer(broker)
        
        # State
        self.running = False
        self.processing_task = None
        self.circuit_breaker = CircuitBreaker()
        
        # Metrics
        self.processed_count = 0
        self.collision_count = 0
        self.last_metrics_time = time.time()
    
    async def start(self):
        """Start the compute node."""
        if self.running:
            return
        
        self.running = True
        
        # Start consumers
        await self.location_consumer.start()
        await self.task_consumer.start()
        
        # Register message handlers
        self.location_consumer.on_message("vehicle-locations", self._handle_location)
        self.task_consumer.on_message("computation-tasks", self._handle_task)
        
        # Start processing task
        self.processing_task = asyncio.create_task(self._process_loop())
        
        logger.info(f"Compute node {self.node_id} started for grid {self.grid_id}")
    
    async def stop(self):
        """Stop the compute node."""
        if not self.running:
            return
        
        self.running = False
        
        # Stop processing task
        if self.processing_task:
            self.processing_task.cancel()
            try:
                await self.processing_task
            except asyncio.CancelledError:
                pass
        
        # Stop consumers
        await self.location_consumer.stop()
        await self.task_consumer.stop()
        
        logger.info(f"Compute node {self.node_id} stopped")
    
    def _handle_location(self, message):
        """
        Handle location data message.
        
        Args:
            message: Location data message
        """
        try:
            data = message.value
            location_data = LocationData(**data)
            
            # Update vehicle state
            if location_data.vehicle_id not in self.vehicle_states:
                self.vehicle_states[location_data.vehicle_id] = VehicleState(location_data.vehicle_id)
            
            self.vehicle_states[location_data.vehicle_id].update(location_data)
            
            # Update spatial index
            self.spatial_index.insert(location_data.vehicle_id, location_data.position)
            
            self.processed_count += 1
        except Exception as e:
            logger.error(f"Error handling location data: {e}")
    
    def _handle_task(self, message):
        """
        Handle computation task message.
        
        Args:
            message: Task message
        """
        try:
            data = message.value
            task = Task(**data)
            
            # Process task based on type
            if task.task_type == "collision_detection":
                self._process_collision_detection_task(task)
            elif task.task_type == "vehicle_count":
                self._process_vehicle_count_task(task)
            else:
                logger.warning(f"Unknown task type: {task.task_type}")
        except Exception as e:
            logger.error(f"Error handling task: {e}")
    
    def _process_collision_detection_task(self, task: Task):
        """
        Process collision detection task.
        
        Args:
            task: Collision detection task
        """
        try:
            vehicle_id = task.data.get("vehicle_id")
            if not vehicle_id or vehicle_id not in self.vehicle_states:
                asyncio.create_task(self._send_error_result(
                    task.task_id, f"Vehicle {vehicle_id} not found"
                ))
                return
            
            vehicle = self.vehicle_states[vehicle_id]
            
            # Get nearby vehicles
            vehicle_loc = vehicle.get_current_location()
            if not vehicle_loc:
                asyncio.create_task(self._send_error_result(
                    task.task_id, f"No location data for vehicle {vehicle_id}"
                ))
                return
            
            nearby_ids = self.spatial_index.query_nearby(
                vehicle_loc.position, self.config.search_radius
            )
            
            nearby_vehicles = {}
            for nearby_id in nearby_ids:
                if nearby_id in self.vehicle_states:
                    nearby_vehicles[nearby_id] = self.vehicle_states[nearby_id]
            
            # Detect collisions
            with Timer(f"collision_detection_{task.task_id}"):
                risks = self.collision_detector.detect_collisions(vehicle, nearby_vehicles)
            
            # Send result
            self.collision_count += len(risks)
            asyncio.create_task(self._send_success_result(
                task.task_id,
                {
                    "vehicle_id": vehicle_id,
                    "collision_risks": [risk.__dict__ for risk in risks],
                    "nearby_count": len(nearby_vehicles)
                }
            ))
        except Exception as e:
            logger.error(f"Error processing collision detection task: {e}")
            asyncio.create_task(self._send_error_result(
                task.task_id, f"Error: {str(e)}"
            ))
    
    def _process_vehicle_count_task(self, task: Task):
        """
        Process vehicle count task.
        
        Args:
            task: Vehicle count task
        """
        try:
            count = self.spatial_index.get_vehicle_count()
            asyncio.create_task(self._send_success_result(
                task.task_id,
                {
                    "vehicle_count": count,
                    "grid_id": self.grid_id
                }
            ))
        except Exception as e:
            logger.error(f"Error processing vehicle count task: {e}")
            asyncio.create_task(self._send_error_result(
                task.task_id, f"Error: {str(e)}"
            ))
    
    async def _send_success_result(self, task_id: str, result: Dict[str, Any]):
        """
        Send successful task result.
        
        Args:
            task_id: Task ID
            result: Task result
        """
        task_result = TaskResult.success_result(
            task_id=task_id,
            result=result,
            processing_time=0.0  # TODO: Track actual processing time
        )
        await self.result_producer.send_result(task_result)
    
    async def _send_error_result(self, task_id: str, error: str):
        """
        Send error task result.
        
        Args:
            task_id: Task ID
            error: Error message
        """
        task_result = TaskResult.error_result(
            task_id=task_id,
            error=error,
            processing_time=0.0  # TODO: Track actual processing time
        )
        await self.result_producer.send_result(task_result)
    
    async def _process_loop(self):
        """Main processing loop."""
        while self.running:
            try:
                # Perform periodic processing
                await self._detect_collisions_for_all()
                
                # Log metrics
                now = time.time()
                if now - self.last_metrics_time >= 10.0:
                    elapsed = now - self.last_metrics_time
                    logger.info(
                        f"Node {self.node_id} metrics: "
                        f"vehicles={len(self.vehicle_states)}, "
                        f"processed={self.processed_count}, "
                        f"rate={self.processed_count/elapsed:.1f}/s, "
                        f"collisions={self.collision_count}"
                    )
                    self.processed_count = 0
                    self.collision_count = 0
                    self.last_metrics_time = now
                
                # Sleep for processing interval
                await asyncio.sleep(self.config.processing_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in processing loop: {e}")
                await asyncio.sleep(1.0)  # Sleep on error to avoid tight loop
    
    async def _detect_collisions_for_all(self):
        """Detect collisions for all vehicles."""
        # Skip if circuit breaker is open
        if not self.circuit_breaker.allow_request():
            logger.warning("Circuit breaker open, skipping collision detection")
            return
        
        try:
            # Process in batches to avoid blocking
            vehicle_ids = list(self.vehicle_states.keys())
            batch_size = min(100, len(vehicle_ids))
            
            for i in range(0, len(vehicle_ids), batch_size):
                batch = vehicle_ids[i:i+batch_size]
                for vehicle_id in batch:
                    if vehicle_id in self.vehicle_states:
                        vehicle = self.vehicle_states[vehicle_id]
                        vehicle_loc = vehicle.get_current_location()
                        
                        if not vehicle_loc:
                            continue
                        
                        # Skip if last update is too old
                        if time.time() - vehicle.last_update > 10.0:
                            continue
                        
                        # Get nearby vehicles
                        nearby_ids = self.spatial_index.query_nearby(
                            vehicle_loc.position, self.config.search_radius
                        )
                        
                        nearby_vehicles = {}
                        for nearby_id in nearby_ids:
                            if nearby_id in self.vehicle_states:
                                nearby_vehicles[nearby_id] = self.vehicle_states[nearby_id]
                        
                        # Detect collisions
                        risks = self.collision_detector.detect_collisions(vehicle, nearby_vehicles)
                        
                        # Process risks (in a real system, would publish to a topic)
                        self.collision_count += len(risks)
                        if risks:
                            logger.debug(f"Detected {len(risks)} collision risks for vehicle {vehicle_id}")
                
                # Yield to other tasks
                await asyncio.sleep(0)
            
            self.circuit_breaker.record_success()
        except Exception as e:
            logger.error(f"Error detecting collisions: {e}")
            self.circuit_breaker.record_failure()


class ComputeNodeFactory:
    """Factory for creating compute nodes."""
    
    @staticmethod
    def create_node(grid_id: str, broker: MessageBroker, 
                   config: Optional[NodeConfig] = None) -> ComputeNode:
        """
        Create a new compute node.
        
        Args:
            grid_id: Grid ID
            broker: Message broker
            config: Node configuration (optional)
            
        Returns:
            New compute node
        """
        node_id = f"node-{grid_id}-{str(uuid.uuid4())[:8]}"
        
        if config is None:
            config = NodeConfig(
                max_workers=4,
                search_radius=100.0,
                batch_size=100,
                processing_interval=0.1
            )
        
        return ComputeNode(node_id, grid_id, broker, config)
