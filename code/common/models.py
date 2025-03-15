"""
Common data models for the distributed platform.
"""
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Tuple
import time
import uuid


@dataclass
class Position:
    """3D position representation."""
    x: float
    y: float
    z: float

    def distance_to(self, other: 'Position') -> float:
        """Calculate Euclidean distance to another position."""
        return ((self.x - other.x) ** 2 + 
                (self.y - other.y) ** 2 + 
                (self.z - other.z) ** 2) ** 0.5


@dataclass
class Vector:
    """3D vector representation."""
    x: float
    y: float
    z: float

    def magnitude(self) -> float:
        """Calculate vector magnitude."""
        return (self.x ** 2 + self.y ** 2 + self.z ** 2) ** 0.5
    
    def normalize(self) -> 'Vector':
        """Return normalized vector."""
        mag = self.magnitude()
        if mag == 0:
            return Vector(0, 0, 0)
        return Vector(self.x / mag, self.y / mag, self.z / mag)


@dataclass
class LocationData:
    """Vehicle location data."""
    vehicle_id: str
    timestamp: float
    position: Position
    velocity: Vector
    heading: float  # in degrees
    vehicle_type: str
    
    @classmethod
    def create(cls, vehicle_id: str, position: Position, 
               velocity: Vector, heading: float, vehicle_type: str) -> 'LocationData':
        """Factory method to create LocationData with current timestamp."""
        return cls(
            vehicle_id=vehicle_id,
            timestamp=time.time(),
            position=position,
            velocity=velocity,
            heading=heading,
            vehicle_type=vehicle_type
        )


@dataclass
class GridConfig:
    """Configuration for spatial grid."""
    initial_size: float
    min_size: float
    max_size: float
    adjustment_threshold: float  # density threshold for grid size adjustment
    adjustment_factor: float  # factor to adjust grid size


@dataclass
class GridInfo:
    """Information about a spatial grid."""
    grid_id: str
    center: Position
    size: float
    vehicle_count: int
    density: float  # vehicles per cubic unit


@dataclass
class NodeConfig:
    """Configuration for compute node."""
    max_workers: int
    search_radius: float
    batch_size: int
    processing_interval: float  # in seconds


@dataclass
class NodeInfo:
    """Information about a compute node."""
    node_id: str
    host: str
    port: int
    status: str  # "active", "starting", "stopping", "inactive"
    grid_ids: List[str]  # list of grid IDs this node is responsible for
    load: float  # current load (0.0-1.0)
    capacity: int  # maximum number of vehicles this node can handle


@dataclass
class CollisionRisk:
    """Representation of a potential collision risk."""
    risk_id: str
    timestamp: float
    vehicle_id1: str
    vehicle_id2: str
    risk_level: float  # 0.0-1.0
    estimated_collision_time: float  # timestamp
    position: Position  # estimated collision position
    relative_velocity: float  # magnitude of relative velocity
    time_to_collision: float  # in seconds
    
    @classmethod
    def create(cls, vehicle_id1: str, vehicle_id2: str, 
               risk_level: float, estimated_collision_time: float,
               position: Position, relative_velocity: float) -> 'CollisionRisk':
        """Factory method to create CollisionRisk with generated ID and current timestamp."""
        return cls(
            risk_id=str(uuid.uuid4()),
            timestamp=time.time(),
            vehicle_id1=vehicle_id1,
            vehicle_id2=vehicle_id2,
            risk_level=risk_level,
            estimated_collision_time=estimated_collision_time,
            position=position,
            relative_velocity=relative_velocity,
            time_to_collision=estimated_collision_time - time.time()
        )


@dataclass
class Task:
    """Representation of a computation task."""
    task_id: str
    task_type: str
    priority: int  # 0-100, higher is more important
    data: Dict[str, Any]
    created_at: float
    timeout: float  # in seconds
    
    @classmethod
    def create(cls, task_type: str, priority: int, 
               data: Dict[str, Any], timeout: float = 60.0) -> 'Task':
        """Factory method to create Task with generated ID and current timestamp."""
        return cls(
            task_id=str(uuid.uuid4()),
            task_type=task_type,
            priority=priority,
            data=data,
            created_at=time.time(),
            timeout=timeout
        )


@dataclass
class TaskResult:
    """Result of a computation task."""
    task_id: str
    success: bool
    result: Optional[Dict[str, Any]]
    error: Optional[str]
    processing_time: float  # in seconds
    completed_at: float
    
    @classmethod
    def success_result(cls, task_id: str, result: Dict[str, Any], 
                      processing_time: float) -> 'TaskResult':
        """Create a successful task result."""
        return cls(
            task_id=task_id,
            success=True,
            result=result,
            error=None,
            processing_time=processing_time,
            completed_at=time.time()
        )
    
    @classmethod
    def error_result(cls, task_id: str, error: str, 
                    processing_time: float) -> 'TaskResult':
        """Create an error task result."""
        return cls(
            task_id=task_id,
            success=False,
            result=None,
            error=error,
            processing_time=processing_time,
            completed_at=time.time()
        )


@dataclass
class LoadMetrics:
    """Metrics about system load."""
    cpu_usage: float  # 0.0-1.0
    memory_usage: float  # 0.0-1.0
    queue_size: int
    processing_rate: float  # tasks per second
    average_latency: float  # in milliseconds
