"""
API implementation for the distributed platform.
"""
import asyncio
import json
import time
from typing import Dict, List, Optional, Any, Union
import uuid

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from ..common.models import (
    LocationData, Position, Vector, CollisionRisk, 
    Task, TaskResult, NodeInfo, LoadMetrics
)
from ..common.utils import get_logger
from ..messaging.messaging import MessageBroker, LocationDataProducer
from ..scheduler.scheduler import Scheduler, SchedulerClient
from ..storage.storage import (
    StorageInterface, InMemoryStorage, 
    VehicleLocationStorage, CollisionRiskStorage, GridVehicleStorage
)

logger = get_logger(__name__)

# Pydantic models for API
class PositionModel(BaseModel):
    x: float
    y: float
    z: float

class VectorModel(BaseModel):
    x: float
    y: float
    z: float

class LocationDataModel(BaseModel):
    vehicle_id: str
    timestamp: Optional[float] = None
    position: PositionModel
    velocity: VectorModel
    heading: float
    vehicle_type: str

class CollisionRiskModel(BaseModel):
    risk_id: Optional[str] = None
    timestamp: Optional[float] = None
    vehicle_id1: str
    vehicle_id2: str
    risk_level: float
    estimated_collision_time: float
    position: PositionModel
    relative_velocity: float
    time_to_collision: Optional[float] = None

class TaskModel(BaseModel):
    task_id: Optional[str] = None
    task_type: str
    priority: int = 50
    data: Dict[str, Any]
    created_at: Optional[float] = None
    timeout: float = 60.0

class NodeInfoModel(BaseModel):
    node_id: str
    host: str
    port: int
    status: str
    grid_ids: List[str]
    load: float
    capacity: int

class LoadMetricsModel(BaseModel):
    cpu_usage: float
    memory_usage: float
    queue_size: int
    processing_rate: float
    average_latency: float

class ApiResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Any] = None


class ApiServer:
    """API server for the distributed platform."""
    
    def __init__(self, broker: MessageBroker, scheduler: Scheduler, 
                storage: StorageInterface):
        """
        Initialize API server.
        
        Args:
            broker: Message broker
            scheduler: Task scheduler
            storage: Storage interface
        """
        self.broker = broker
        self.scheduler = scheduler
        self.storage = storage
        
        # Storage interfaces
        self.location_storage = VehicleLocationStorage(storage)
        self.risk_storage = CollisionRiskStorage(storage)
        self.grid_storage = GridVehicleStorage(storage)
        
        # Messaging
        self.location_producer = LocationDataProducer(broker)
        
        # FastAPI app
        self.app = FastAPI(
            title="Distributed Platform API",
            description="API for the distributed real-time computation platform",
            version="1.0.0"
        )
        
        # Add CORS middleware
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        # Register routes
        self._register_routes()
    
    def _register_routes(self):
        """Register API routes."""
        # Health check
        @self.app.get("/health", response_model=ApiResponse, tags=["System"])
        async def health_check():
            return {
                "success": True,
                "message": "Service is healthy",
                "data": {
                    "timestamp": time.time(),
                    "status": "ok"
                }
            }
        
        # Vehicle location
        @self.app.post("/vehicles/location", response_model=ApiResponse, tags=["Vehicles"])
        async def update_vehicle_location(location: LocationDataModel):
            # Convert to internal model
            location_data = LocationData(
                vehicle_id=location.vehicle_id,
                timestamp=location.timestamp or time.time(),
                position=Position(
                    x=location.position.x,
                    y=location.position.y,
                    z=location.position.z
                ),
                velocity=Vector(
                    x=location.velocity.x,
                    y=location.velocity.y,
                    z=location.velocity.z
                ),
                heading=location.heading,
                vehicle_type=location.vehicle_type
            )
            
            # Send to message broker
            success = await self.location_producer.send_location(location_data)
            
            # Store in storage
            await self.location_storage.store_location(location_data)
            
            return {
                "success": success,
                "message": "Location updated" if success else "Failed to update location",
                "data": None
            }
        
        @self.app.get("/vehicles/{vehicle_id}/location", response_model=ApiResponse, tags=["Vehicles"])
        async def get_vehicle_location(vehicle_id: str):
            location = await self.location_storage.get_location(vehicle_id)
            
            if not location:
                raise HTTPException(status_code=404, detail=f"Vehicle {vehicle_id} not found")
            
            return {
                "success": True,
                "message": "Location retrieved",
                "data": {
                    "vehicle_id": location.vehicle_id,
                    "timestamp": location.timestamp,
                    "position": {
                        "x": location.position.x,
                        "y": location.position.y,
                        "z": location.position.z
                    },
                    "velocity": {
                        "x": location.velocity.x,
                        "y": location.velocity.y,
                        "z": location.velocity.z
                    },
                    "heading": location.heading,
                    "vehicle_type": location.vehicle_type
                }
            }
        
        @self.app.get("/vehicles/{vehicle_id}/history", response_model=ApiResponse, tags=["Vehicles"])
        async def get_vehicle_history(vehicle_id: str):
            history = await self.location_storage.get_location_history(vehicle_id)
            
            if not history:
                raise HTTPException(status_code=404, detail=f"No history for vehicle {vehicle_id}")
            
            return {
                "success": True,
                "message": "History retrieved",
                "data": [
                    {
                        "vehicle_id": loc.vehicle_id,
                        "timestamp": loc.timestamp,
                        "position": {
                            "x": loc.position.x,
                            "y": loc.position.y,
                            "z": loc.position.z
                        },
                        "velocity": {
                            "x": loc.velocity.x,
                            "y": loc.velocity.y,
                            "z": loc.velocity.z
                        },
                        "heading": loc.heading,
                        "vehicle_type": loc.vehicle_type
                    }
                    for loc in history
                ]
            }
        
        # Collision risks
        @self.app.get("/vehicles/{vehicle_id}/risks", response_model=ApiResponse, tags=["Risks"])
        async def get_vehicle_risks(vehicle_id: str):
            risks = await self.risk_storage.get_vehicle_risks(vehicle_id)
            
            return {
                "success": True,
                "message": f"Retrieved {len(risks)} risks",
                "data": [
                    {
                        "risk_id": risk.risk_id,
                        "timestamp": risk.timestamp,
                        "vehicle_id1": risk.vehicle_id1,
                        "vehicle_id2": risk.vehicle_id2,
                        "risk_level": risk.risk_level,
                        "estimated_collision_time": risk.estimated_collision_time,
                        "position": {
                            "x": risk.position.x,
                            "y": risk.position.y,
                            "z": risk.position.z
                        },
                        "relative_velocity": risk.relative_velocity,
                        "time_to_collision": risk.time_to_collision
                    }
                    for risk in risks
                ]
            }
        
        @self.app.get("/risks/{risk_id}", response_model=ApiResponse, tags=["Risks"])
        async def get_risk(risk_id: str):
            risk = await self.risk_storage.get_risk(risk_id)
            
            if not risk:
                raise HTTPException(status_code=404, detail=f"Risk {risk_id} not found")
            
            return {
                "success": True,
                "message": "Risk retrieved",
                "data": {
                    "risk_id": risk.risk_id,
                    "timestamp": risk.timestamp,
                    "vehicle_id1": risk.vehicle_id1,
                    "vehicle_id2": risk.vehicle_id2,
                    "risk_level": risk.risk_level,
                    "estimated_collision_time": risk.estimated_collision_time,
                    "position": {
                        "x": risk.position.x,
                        "y": risk.position.y,
                        "z": risk.position.z
                    },
                    "relative_velocity": risk.relative_velocity,
                    "time_to_collision": risk.time_to_collision
                }
            }
        
        # Tasks
        @self.app.post("/tasks", response_model=ApiResponse, tags=["Tasks"])
        async def submit_task(task_model: TaskModel):
            # Convert to internal model
            task = Task(
                task_id=task_model.task_id or str(uuid.uuid4()),
                task_type=task_model.task_type,
                priority=task_model.priority,
                data=task_model.data,
                created_at=task_model.created_at or time.time(),
                timeout=task_model.timeout
            )
            
            # Submit to scheduler
            success = await self.scheduler.submit_task(task)
            
            return {
                "success": success,
                "message": "Task submitted" if success else "Failed to submit task",
                "data": {
                    "task_id": task.task_id
                }
            }
        
        # Nodes
        @self.app.post("/nodes", response_model=ApiResponse, tags=["Nodes"])
        async def register_node(node_info: NodeInfoModel):
            # Convert to internal model
            node = NodeInfo(
                node_id=node_info.node_id,
                host=node_info.host,
                port=node_info.port,
                status=node_info.status,
                grid_ids=node_info.grid_ids,
                load=node_info.load,
                capacity=node_info.capacity
            )
            
            # Register with scheduler
            self.scheduler.register_node(node)
            
            return {
                "success": True,
                "message": f"Node {node.node_id} registered",
                "data": None
            }
        
        @self.app.delete("/nodes/{node_id}", response_model=ApiResponse, tags=["Nodes"])
        async def unregister_node(node_id: str):
            # Unregister from scheduler
            self.scheduler.unregister_node(node_id)
            
            return {
                "success": True,
                "message": f"Node {node_id} unregistered",
                "data": None
            }
        
        @self.app.post("/nodes/{node_id}/load", response_model=ApiResponse, tags=["Nodes"])
        async def update_node_load(node_id: str, load_metrics: LoadMetricsModel):
            # Convert to internal model
            metrics = LoadMetrics(
                cpu_usage=load_metrics.cpu_usage,
                memory_usage=load_metrics.memory_usage,
                queue_size=load_metrics.queue_size,
                processing_rate=load_metrics.processing_rate,
                average_latency=load_metrics.average_latency
            )
            
            # Update scheduler
            self.scheduler.update_node_load(node_id, metrics)
            
            return {
                "success": True,
                "message": f"Load metrics updated for node {node_id}",
                "data": None
            }
        
        # Grids
        @self.app.get("/grids/{grid_id}/vehicles", response_model=ApiResponse, tags=["Grids"])
        async def get_grid_vehicles(grid_id: str):
            vehicles = await self.grid_storage.get_grid_vehicles(grid_id)
            
            return {
                "success": True,
                "message": f"Retrieved {len(vehicles)} vehicles in grid {grid_id}",
                "data": vehicles
            }
    
    def run(self, host: str = "0.0.0.0", port: int = 8000):
        """
        Run the API server.
        
        Args:
            host: Host to bind to
            port: Port to bind to
        """
        import uvicorn
        uvicorn.run(self.app, host=host, port=port)


class ApiClient:
    """Client for interacting with the API server."""
    
    def __init__(self, base_url: str):
        """
        Initialize API client.
        
        Args:
            base_url: Base URL of the API server
        """
        self.base_url = base_url.rstrip('/')
        self.session = None
    
    async def connect(self):
        """Connect to API server."""
        import aiohttp
        self.session = aiohttp.ClientSession()
    
    async def disconnect(self):
        """Disconnect from API server."""
        if self.session:
            await self.session.close()
            self.session = None
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Check API server health.
        
        Returns:
            Health check response
        """
        return await self._get("/health")
    
    async def update_vehicle_location(self, location: LocationDataModel) -> Dict[str, Any]:
        """
        Update vehicle location.
        
        Args:
            location: Vehicle location data
            
        Returns:
            API response
        """
        return await self._post("/vehicles/location", location.dict())
    
    async def get_vehicle_location(self, vehicle_id: str) -> Dict[str, Any]:
        """
        Get vehicle location.
        
        Args:
            vehicle_id: Vehicle ID
            
        Returns:
            API response
        """
        return await self._get(f"/vehicles/{vehicle_id}/location")
    
    async def get_vehicle_history(self, vehicle_id: str) -> Dict[str, Any]:
        """
        Get vehicle location history.
        
        Args:
            vehicle_id: Vehicle ID
            
        Returns:
            API response
        """
        return await self._get(f"/vehicles/{vehicle_id}/history")
    
    async def get_vehicle_risks(self, vehicle_id: str) -> Dict[str, Any]:
        """
        Get vehicle collision risks.
        
        Args:
            vehicle_id: Vehicle ID
            
        Returns:
            API response
        """
        return await self._get(f"/vehicles/{vehicle_id}/risks")
    
    async def get_risk(self, risk_id: str) -> Dict[str, Any]:
        """
        Get collision risk.
        
        Args:
            risk_id: Risk ID
            
        Returns:
            API response
        """
        return await self._get(f"/risks/{risk_id}")
    
    async def submit_task(self, task: TaskModel) -> Dict[str, Any]:
        """
        Submit task.
        
        Args:
            task: Task data
            
        Returns:
            API response
        """
        return await self._post("/tasks", task.dict())
    
    async def register_node(self, node_info: NodeInfoModel) -> Dict[str, Any]:
        """
        Register compute node.
        
        Args:
            node_info: Node information
            
        Returns:
            API response
        """
        return await self._post("/nodes", node_info.dict())
    
    async def unregister_node(self, node_id: str) -> Dict[str, Any]:
        """
        Unregister compute node.
        
        Args:
            node_id: Node ID
            
        Returns:
            API response
        """
        return await self._delete(f"/nodes/{node_id}")
    
    async def update_node_load(self, node_id: str, load_metrics: LoadMetricsModel) -> Dict[str, Any]:
        """
        Update node load metrics.
        
        Args:
            node_id: Node ID
            load_metrics: Load metrics
            
        Returns:
            API response
        """
        return await self._post(f"/nodes/{node_id}/load", load_metrics.dict())
    
    async def get_grid_vehicles(self, grid_id: str) -> Dict[str, Any]:
        """
        Get vehicles in grid.
        
        Args:
            grid_id: Grid ID
            
        Returns:
            API response
        """
        return await self._get(f"/grids/{grid_id}/vehicles")
    
    async def _get(self, path: str) -> Dict[str, Any]:
        """
        Send GET request.
        
        Args:
            path: API path
            
        Returns:
            API response
        """
        if not self.session:
            await self.connect()
        
        url = f"{self.base_url}{path}"
        async with self.session.get(url) as response:
            return await response.json()
    
    async def _post(self, path: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send POST request.
        
        Args:
            path: API path
            data: Request data
            
        Returns:
            API response
        """
        if not self.session:
            await self.connect()
        
        url = f"{self.base_url}{path}"
        async with self.session.post(url, json=data) as response:
            return await response.json()
    
    async def _delete(self, path: str) -> Dict[str, Any]:
        """
        Send DELETE request.
        
        Args:
            path: API path
            
        Returns:
            API response
        """
        if not self.session:
            await self.connect()
        
        url = f"{self.base_url}{path}"
        async with self.session.delete(url) as response:
            return await response.json()
