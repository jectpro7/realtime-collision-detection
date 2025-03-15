"""
Storage interfaces for the distributed platform.
"""
import asyncio
import json
import time
from typing import Dict, List, Optional, Any, Tuple, Union
import uuid

from ..common.models import LocationData, CollisionRisk, Position, Vector
from ..common.utils import get_logger, Timer, CircuitBreaker

logger = get_logger(__name__)

class StorageInterface:
    """Base interface for storage implementations."""
    
    async def connect(self):
        """Connect to storage."""
        raise NotImplementedError
    
    async def disconnect(self):
        """Disconnect from storage."""
        raise NotImplementedError
    
    async def is_connected(self) -> bool:
        """Check if connected to storage."""
        raise NotImplementedError


class InMemoryStorage(StorageInterface):
    """In-memory storage implementation for testing and development."""
    
    def __init__(self):
        """Initialize in-memory storage."""
        self.data = {}
        self.connected = False
    
    async def connect(self):
        """Connect to storage."""
        self.connected = True
        logger.info("Connected to in-memory storage")
    
    async def disconnect(self):
        """Disconnect from storage."""
        self.connected = False
        logger.info("Disconnected from in-memory storage")
    
    async def is_connected(self) -> bool:
        """Check if connected to storage."""
        return self.connected
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set key-value pair.
        
        Args:
            key: Key
            value: Value
            ttl: Time-to-live in seconds (optional)
            
        Returns:
            True if successful, False otherwise
        """
        if not self.connected:
            return False
        
        self.data[key] = {
            'value': value,
            'expires': time.time() + ttl if ttl else None
        }
        return True
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Get value for key.
        
        Args:
            key: Key
            
        Returns:
            Value or None if not found or expired
        """
        if not self.connected:
            return None
        
        if key not in self.data:
            return None
        
        item = self.data[key]
        
        # Check if expired
        if item['expires'] and time.time() > item['expires']:
            del self.data[key]
            return None
        
        return item['value']
    
    async def delete(self, key: str) -> bool:
        """
        Delete key.
        
        Args:
            key: Key
            
        Returns:
            True if deleted, False otherwise
        """
        if not self.connected:
            return False
        
        if key in self.data:
            del self.data[key]
            return True
        return False
    
    async def exists(self, key: str) -> bool:
        """
        Check if key exists.
        
        Args:
            key: Key
            
        Returns:
            True if exists and not expired, False otherwise
        """
        if not self.connected:
            return False
        
        if key not in self.data:
            return False
        
        item = self.data[key]
        
        # Check if expired
        if item['expires'] and time.time() > item['expires']:
            del self.data[key]
            return False
        
        return True


class VehicleLocationStorage:
    """Storage for vehicle location data."""
    
    def __init__(self, storage: StorageInterface):
        """
        Initialize vehicle location storage.
        
        Args:
            storage: Storage interface
        """
        self.storage = storage
        self.circuit_breaker = CircuitBreaker()
    
    async def store_location(self, location_data: LocationData) -> bool:
        """
        Store vehicle location.
        
        Args:
            location_data: Vehicle location data
            
        Returns:
            True if stored, False otherwise
        """
        if not self.circuit_breaker.allow_request():
            logger.warning("Circuit breaker open, skipping location storage")
            return False
        
        try:
            # Store current location
            key = f"vehicle:{location_data.vehicle_id}:location"
            success = await self.storage.set(key, location_data.__dict__, ttl=60)
            
            # Store in history
            history_key = f"vehicle:{location_data.vehicle_id}:history"
            history = await self.storage.get(history_key) or []
            history.append(location_data.__dict__)
            
            # Keep only last 10 positions
            if len(history) > 10:
                history = history[-10:]
            
            await self.storage.set(history_key, history, ttl=3600)
            
            self.circuit_breaker.record_success()
            return success
        except Exception as e:
            logger.error(f"Error storing location: {e}")
            self.circuit_breaker.record_failure()
            return False
    
    async def get_location(self, vehicle_id: str) -> Optional[LocationData]:
        """
        Get current vehicle location.
        
        Args:
            vehicle_id: Vehicle ID
            
        Returns:
            Location data or None if not found
        """
        if not self.circuit_breaker.allow_request():
            logger.warning("Circuit breaker open, skipping location retrieval")
            return None
        
        try:
            key = f"vehicle:{vehicle_id}:location"
            data = await self.storage.get(key)
            
            if data:
                self.circuit_breaker.record_success()
                return LocationData(**data)
            
            self.circuit_breaker.record_success()
            return None
        except Exception as e:
            logger.error(f"Error getting location: {e}")
            self.circuit_breaker.record_failure()
            return None
    
    async def get_location_history(self, vehicle_id: str) -> List[LocationData]:
        """
        Get vehicle location history.
        
        Args:
            vehicle_id: Vehicle ID
            
        Returns:
            List of location data
        """
        if not self.circuit_breaker.allow_request():
            logger.warning("Circuit breaker open, skipping history retrieval")
            return []
        
        try:
            key = f"vehicle:{vehicle_id}:history"
            data = await self.storage.get(key) or []
            
            history = [LocationData(**item) for item in data]
            
            self.circuit_breaker.record_success()
            return history
        except Exception as e:
            logger.error(f"Error getting location history: {e}")
            self.circuit_breaker.record_failure()
            return []


class CollisionRiskStorage:
    """Storage for collision risk data."""
    
    def __init__(self, storage: StorageInterface):
        """
        Initialize collision risk storage.
        
        Args:
            storage: Storage interface
        """
        self.storage = storage
        self.circuit_breaker = CircuitBreaker()
    
    async def store_risk(self, risk: CollisionRisk) -> bool:
        """
        Store collision risk.
        
        Args:
            risk: Collision risk data
            
        Returns:
            True if stored, False otherwise
        """
        if not self.circuit_breaker.allow_request():
            logger.warning("Circuit breaker open, skipping risk storage")
            return False
        
        try:
            # Store risk
            key = f"risk:{risk.risk_id}"
            success = await self.storage.set(key, risk.__dict__, ttl=3600)
            
            # Add to vehicle risks
            for vehicle_id in [risk.vehicle_id1, risk.vehicle_id2]:
                vehicle_risks_key = f"vehicle:{vehicle_id}:risks"
                vehicle_risks = await self.storage.get(vehicle_risks_key) or []
                vehicle_risks.append(risk.risk_id)
                
                # Keep only recent risks
                if len(vehicle_risks) > 20:
                    vehicle_risks = vehicle_risks[-20:]
                
                await self.storage.set(vehicle_risks_key, vehicle_risks, ttl=3600)
            
            self.circuit_breaker.record_success()
            return success
        except Exception as e:
            logger.error(f"Error storing risk: {e}")
            self.circuit_breaker.record_failure()
            return False
    
    async def get_risk(self, risk_id: str) -> Optional[CollisionRisk]:
        """
        Get collision risk.
        
        Args:
            risk_id: Risk ID
            
        Returns:
            Collision risk or None if not found
        """
        if not self.circuit_breaker.allow_request():
            logger.warning("Circuit breaker open, skipping risk retrieval")
            return None
        
        try:
            key = f"risk:{risk_id}"
            data = await self.storage.get(key)
            
            if data:
                self.circuit_breaker.record_success()
                return CollisionRisk(**data)
            
            self.circuit_breaker.record_success()
            return None
        except Exception as e:
            logger.error(f"Error getting risk: {e}")
            self.circuit_breaker.record_failure()
            return None
    
    async def get_vehicle_risks(self, vehicle_id: str) -> List[CollisionRisk]:
        """
        Get vehicle collision risks.
        
        Args:
            vehicle_id: Vehicle ID
            
        Returns:
            List of collision risks
        """
        if not self.circuit_breaker.allow_request():
            logger.warning("Circuit breaker open, skipping risks retrieval")
            return []
        
        try:
            vehicle_risks_key = f"vehicle:{vehicle_id}:risks"
            risk_ids = await self.storage.get(vehicle_risks_key) or []
            
            risks = []
            for risk_id in risk_ids:
                risk = await self.get_risk(risk_id)
                if risk:
                    risks.append(risk)
            
            self.circuit_breaker.record_success()
            return risks
        except Exception as e:
            logger.error(f"Error getting vehicle risks: {e}")
            self.circuit_breaker.record_failure()
            return []


class GridVehicleStorage:
    """Storage for grid-vehicle mappings."""
    
    def __init__(self, storage: StorageInterface):
        """
        Initialize grid-vehicle storage.
        
        Args:
            storage: Storage interface
        """
        self.storage = storage
        self.circuit_breaker = CircuitBreaker()
    
    async def add_vehicle_to_grid(self, grid_id: str, vehicle_id: str) -> bool:
        """
        Add vehicle to grid.
        
        Args:
            grid_id: Grid ID
            vehicle_id: Vehicle ID
            
        Returns:
            True if added, False otherwise
        """
        if not self.circuit_breaker.allow_request():
            logger.warning("Circuit breaker open, skipping grid update")
            return False
        
        try:
            # Add to grid vehicles
            grid_key = f"grid:{grid_id}:vehicles"
            grid_vehicles = await self.storage.get(grid_key) or []
            
            if vehicle_id not in grid_vehicles:
                grid_vehicles.append(vehicle_id)
                await self.storage.set(grid_key, grid_vehicles, ttl=3600)
            
            # Update vehicle grid
            vehicle_grid_key = f"vehicle:{vehicle_id}:grid"
            await self.storage.set(vehicle_grid_key, grid_id, ttl=3600)
            
            self.circuit_breaker.record_success()
            return True
        except Exception as e:
            logger.error(f"Error adding vehicle to grid: {e}")
            self.circuit_breaker.record_failure()
            return False
    
    async def remove_vehicle_from_grid(self, grid_id: str, vehicle_id: str) -> bool:
        """
        Remove vehicle from grid.
        
        Args:
            grid_id: Grid ID
            vehicle_id: Vehicle ID
            
        Returns:
            True if removed, False otherwise
        """
        if not self.circuit_breaker.allow_request():
            logger.warning("Circuit breaker open, skipping grid update")
            return False
        
        try:
            # Remove from grid vehicles
            grid_key = f"grid:{grid_id}:vehicles"
            grid_vehicles = await self.storage.get(grid_key) or []
            
            if vehicle_id in grid_vehicles:
                grid_vehicles.remove(vehicle_id)
                await self.storage.set(grid_key, grid_vehicles, ttl=3600)
            
            # Clear vehicle grid
            vehicle_grid_key = f"vehicle:{vehicle_id}:grid"
            await self.storage.delete(vehicle_grid_key)
            
            self.circuit_breaker.record_success()
            return True
        except Exception as e:
            logger.error(f"Error removing vehicle from grid: {e}")
            self.circuit_breaker.record_failure()
            return False
    
    async def get_grid_vehicles(self, grid_id: str) -> List[str]:
        """
        Get vehicles in grid.
        
        Args:
            grid_id: Grid ID
            
        Returns:
            List of vehicle IDs
        """
        if not self.circuit_breaker.allow_request():
            logger.warning("Circuit breaker open, skipping grid retrieval")
            return []
        
        try:
            grid_key = f"grid:{grid_id}:vehicles"
            vehicles = await self.storage.get(grid_key) or []
            
            self.circuit_breaker.record_success()
            return vehicles
        except Exception as e:
            logger.error(f"Error getting grid vehicles: {e}")
            self.circuit_breaker.record_failure()
            return []
    
    async def get_vehicle_grid(self, vehicle_id: str) -> Optional[str]:
        """
        Get grid for vehicle.
        
        Args:
            vehicle_id: Vehicle ID
            
        Returns:
            Grid ID or None if not found
        """
        if not self.circuit_breaker.allow_request():
            logger.warning("Circuit breaker open, skipping grid retrieval")
            return None
        
        try:
            vehicle_grid_key = f"vehicle:{vehicle_id}:grid"
            grid_id = await self.storage.get(vehicle_grid_key)
            
            self.circuit_breaker.record_success()
            return grid_id
        except Exception as e:
            logger.error(f"Error getting vehicle grid: {e}")
            self.circuit_breaker.record_failure()
            return None


class StorageFactory:
    """Factory for creating storage instances."""
    
    @staticmethod
    def create_in_memory_storage() -> InMemoryStorage:
        """
        Create in-memory storage.
        
        Returns:
            In-memory storage instance
        """
        return InMemoryStorage()
    
    @staticmethod
    def create_vehicle_location_storage(storage: StorageInterface) -> VehicleLocationStorage:
        """
        Create vehicle location storage.
        
        Args:
            storage: Storage interface
            
        Returns:
            Vehicle location storage instance
        """
        return VehicleLocationStorage(storage)
    
    @staticmethod
    def create_collision_risk_storage(storage: StorageInterface) -> CollisionRiskStorage:
        """
        Create collision risk storage.
        
        Args:
            storage: Storage interface
            
        Returns:
            Collision risk storage instance
        """
        return CollisionRiskStorage(storage)
    
    @staticmethod
    def create_grid_vehicle_storage(storage: StorageInterface) -> GridVehicleStorage:
        """
        Create grid-vehicle storage.
        
        Args:
            storage: Storage interface
            
        Returns:
            Grid-vehicle storage instance
        """
        return GridVehicleStorage(storage)
