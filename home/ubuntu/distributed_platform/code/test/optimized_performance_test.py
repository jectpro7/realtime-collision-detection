"""
优化版性能测试脚本，针对分布式实时计算平台的性能瓶颈进行优化
"""
import os
import sys
import time
import random
import json
import asyncio
import argparse
from datetime import datetime
from typing import Dict, List, Any, Set
from dataclasses import dataclass, field
import multiprocessing
import threading
import numpy as np
import matplotlib.pyplot as plt

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# 导入系统组件
from code.common.models import Position, Vector


@dataclass
class Vehicle:
    """车辆数据模型"""
    id: str
    position: Position
    velocity: Vector
    acceleration: Vector
    heading: float  # 航向角（弧度）
    size: float  # 车辆尺寸（米）
    type: str  # 车辆类型
    timestamp: float  # 时间戳


@dataclass
class PerformanceMetrics:
    """性能指标"""
    throughput: float = 0.0  # 吞吐量（TPS）
    avg_latency: float = 0.0  # 平均延迟（毫秒）
    p95_latency: float = 0.0  # P95延迟（毫秒）
    p99_latency: float = 0.0  # P99延迟（毫秒）
    max_latency: float = 0.0  # 最大延迟（毫秒）
    error_rate: float = 0.0  # 错误率
    cpu_usage: float = 0.0  # CPU使用率
    memory_usage: float = 0.0  # 内存使用率
    timestamp: float = field(default_factory=time.time)  # 时间戳


class VehicleGenerator:
    """车辆生成器，生成模拟车辆数据"""
    
    def __init__(self, num_vehicles: int = 1000, map_size: tuple = (10000, 10000)):
        """
        初始化车辆生成器
        
        Args:
            num_vehicles: 车辆数量
            map_size: 地图大小 (width, height)
        """
        self.num_vehicles = num_vehicles
        self.map_size = map_size
        self.vehicles: Dict[str, Vehicle] = {}
        
        # 车辆类型
        self.vehicle_types = ["car", "truck", "bus", "motorcycle"]
        self.vehicle_sizes = {"car": 2.0, "truck": 4.0, "bus": 5.0, "motorcycle": 1.0}
        
        # 城市位置（模拟数据倾斜）
        self.cities = [
            (map_size[0] * 0.25, map_size[1] * 0.25, 1000),  # (x, y, radius)
            (map_size[0] * 0.75, map_size[1] * 0.25, 1000),
            (map_size[0] * 0.25, map_size[1] * 0.75, 1000),
            (map_size[0] * 0.75, map_size[1] * 0.75, 1000),
            (map_size[0] * 0.5, map_size[1] * 0.5, 2000),
        ]
    
    def initialize_vehicles(self) -> None:
        """初始化车辆"""
        self.vehicles.clear()
        
        for i in range(self.num_vehicles):
            vehicle_id = f"vehicle-{i}"
            
            # 确定车辆类型
            vehicle_type = random.choice(self.vehicle_types)
            vehicle_size = self.vehicle_sizes[vehicle_type]
            
            # 确定位置（80%在城市附近，20%均匀分布）
            if random.random() < 0.8:
                # 在城市附近
                city = random.choice(self.cities)
                city_x, city_y, city_radius = city
                
                # 在城市半径内随机位置
                r = random.random() * city_radius
                theta = random.random() * 2 * 3.14159
                
                x = city_x + r * np.cos(theta)
                y = city_y + r * np.sin(theta)
                z = 0.0
            else:
                # 均匀分布
                x = random.uniform(0, self.map_size[0])
                y = random.uniform(0, self.map_size[1])
                z = 0.0
            
            position = Position(x=x, y=y, z=z)
            
            # 随机速度和方向
            speed = random.uniform(5, 20)  # 5-20 m/s (18-72 km/h)
            heading = random.uniform(0, 2 * 3.14159)
            
            velocity = Vector(
                x=speed * np.cos(heading),
                y=speed * np.sin(heading),
                z=0
            )
            
            acceleration = Vector(x=0, y=0, z=0)
            
            # 创建车辆对象
            vehicle = Vehicle(
                id=vehicle_id,
                position=position,
                velocity=velocity,
                acceleration=acceleration,
                heading=heading,
                size=vehicle_size,
                type=vehicle_type,
                timestamp=time.time()
            )
            
            self.vehicles[vehicle_id] = vehicle
    
    def update_vehicles(self, time_delta: float) -> List[Vehicle]:
        """
        更新车辆位置
        
        Args:
            time_delta: 时间增量（秒）
            
        Returns:
            更新后的车辆列表
        """
        updated_vehicles = []
        
        for vehicle_id, vehicle in self.vehicles.items():
            # 更新位置
            vehicle.position.x += vehicle.velocity.x * time_delta
            vehicle.position.y += vehicle.velocity.y * time_delta
            vehicle.position.z += vehicle.velocity.z * time_delta
            
            # 边界检查
            if vehicle.position.x < 0:
                vehicle.position.x = 0
                vehicle.velocity.x = -vehicle.velocity.x
            elif vehicle.position.x > self.map_size[0]:
                vehicle.position.x = self.map_size[0]
                vehicle.velocity.x = -vehicle.velocity.x
            
            if vehicle.position.y < 0:
                vehicle.position.y = 0
                vehicle.velocity.y = -vehicle.velocity.y
            elif vehicle.position.y > self.map_size[1]:
                vehicle.position.y = self.map_size[1]
                vehicle.velocity.y = -vehicle.velocity.y
            
            # 随机改变加速度
            if random.random() < 0.1:
                vehicle.acceleration.x = random.uniform(-1, 1)
                vehicle.acceleration.y = random.uniform(-1, 1)
            
            # 更新速度
            vehicle.velocity.x += vehicle.acceleration.x * time_delta
            vehicle.velocity.y += vehicle.acceleration.y * time_delta
            
            # 限制速度
            speed = np.sqrt(vehicle.velocity.x**2 + vehicle.velocity.y**2)
            max_speed = 30.0  # 最大速度 30 m/s (108 km/h)
            if speed > max_speed:
                vehicle.velocity.x = vehicle.velocity.x / speed * max_speed
                vehicle.velocity.y = vehicle.velocity.y / speed * max_speed
            
            # 更新航向角
            if speed > 0.1:
                vehicle.heading = np.arctan2(vehicle.velocity.y, vehicle.velocity.x)
            
            # 更新时间戳
            vehicle.timestamp = time.time()
            
            # 添加到更新列表
            updated_vehicles.append(vehicle)
        
        return updated_vehicles
    
    def get_vehicles(self) -> List[Vehicle]:
        """
        获取所有车辆
        
        Returns:
            车辆列表
        """
        return list(self.vehicles.values())


class GridCell:
    """网格单元，用于存储对象"""
    
    def __init__(self):
        """初始化网格单元"""
        self.objects = {}  # 对象ID -> (位置, 大小)
    
    def add_object(self, object_id: str, position: Position, size: float) -> None:
        """
        添加对象
        
        Args:
            object_id: 对象ID
            position: 对象位置
            size: 对象大小
        """
        self.objects[object_id] = (position, size)
    
    def remove_object(self, object_id: str) -> None:
        """
        移除对象
        
        Args:
            object_id: 对象ID
        """
        if object_id in self.objects:
            del self.objects[object_id]
    
    def get_objects(self) -> Dict[str, tuple]:
        """
        获取所有对象
        
        Returns:
            对象字典 {对象ID: (位置, 大小)}
        """
        return self.objects
    
    def is_empty(self) -> bool:
        """
        检查网格是否为空
        
        Returns:
            是否为空
        """
        return len(self.objects) == 0


class OptimizedSpatialIndex:
    """优化的空间索引，用于快速查找空间中的对象"""
    
    def __init__(self, cell_size: float = 100.0, map_size: tuple = (10000, 10000, 100)):
        """
        初始化空间索引
        
        Args:
            cell_size: 网格单元大小
            map_size: 地图大小 (width, height, depth)
        """
        self.cell_size = cell_size
        self.map_size = map_size
        
        # 计算网格维度
        self.grid_width = int(map_size[0] / cell_size) + 1
        self.grid_height = int(map_size[1] / cell_size) + 1
        self.grid_depth = int(map_size[2] / cell_size) + 1
        
        # 网格数据
        self.grid = {}  # (x, y, z) -> GridCell
        self.object_cells = {}  # 对象ID -> 网格坐标集合
    
    def clear(self) -> None:
        """清空索引"""
        self.grid.clear()
        self.object_cells.clear()
    
    def _get_cell_coords(self, position: Position) -> tuple:
        """
        获取位置所在的网格坐标
        
        Args:
            position: 位置
            
        Returns:
            网格坐标 (x, y, z)
        """
        x = int(position.x / self.cell_size)
        y = int(position.y / self.cell_size)
        z = int(position.z / self.cell_size)
        return (x, y, z)
    
    def _get_cell(self, coords: tuple) -> GridCell:
        """
        获取网格单元
        
        Args:
            coords: 网格坐标
            
        Returns:
            网格单元
        """
        if coords not in self.grid:
            self.grid[coords] = GridCell()
        return self.grid[coords]
    
    def insert(self, object_id: str, position: Position, size: float) -> None:
        """
        插入对象
        
        Args:
            object_id: 对象ID
            position: 对象位置
            size: 对象大小
        """
        # 计算对象所在的网格坐标
        center_coords = self._get_cell_coords(position)
        
        # 计算对象覆盖的网格范围
        radius_cells = max(1, int(size / self.cell_size) + 1)
        
        # 存储对象所在的网格坐标
        if object_id not in self.object_cells:
            self.object_cells[object_id] = set()
        
        # 将对象添加到覆盖的所有网格中
        for dx in range(-radius_cells, radius_cells + 1):
            for dy in range(-radius_cells, radius_cells + 1):
                for dz in range(-radius_cells, radius_cells + 1):
                    # 计算网格坐标
                    x = center_coords[0] + dx
                    y = center_coords[1] + dy
                    z = center_coords[2] + dz
                    
                    # 边界检查
                    if 0 <= x < self.grid_width and 0 <= y < self.grid_height and 0 <= z < self.grid_depth:
                        coords = (x, y, z)
                        
                        # 获取网格单元
                        cell = self._get_cell(coords)
                        
                        # 添加对象到网格单元
                        cell.add_object(object_id, position, size)
                        
                        # 记录对象所在的网格坐标
                        self.object_cells[object_id].add(coords)
    
    def remove(self, object_id: str) -> None:
        """
        移除对象
        
        Args:
            object_id: 对象ID
        """
        if object_id not in self.object_cells:
            return
        
        # 获取对象所在的网格坐标
        coords_set = self.object_cells[object_id]
        
        # 从所有网格中移除对象
        for coords in coords_set:
            if coords in self.grid:
                cell = self.grid[coords]
                cell.remove_object(object_id)
                
                # 如果网格为空，则移除网格
                if cell.is_empty():
                    del self.grid[coords]
        
        # 移除对象记录
        del self.object_cells[object_id]
    
    def update(self, object_id: str, position: Position, size: float) -> None:
        """
        更新对象
        
        Args:
            object_id: 对象ID
            position: 对象位置
            size: 对象大小
        """
        # 移除旧的对象
        self.remove(object_id)
        
        # 插入新的对象
        self.insert(object_id, position, size)
    
    def query(self, position: Position, radius: float) -> List[str]:
        """
        查询指定范围内的对象
        
        Args:
            position: 查询位置
            radius: 查询半径
            
        Returns:
            对象ID列表
        """
        # 计算查询范围覆盖的网格
        center_coords = self._get_cell_coords(position)
        radius_cells = max(1, int(radius / self.cell_size) + 1)
        
        # 收集查询范围内的所有对象
        result_set = set()
        
        for dx in range(-radius_cells, radius_cells + 1):
            for dy in range(-radius_cells, radius_cells + 1):
                for dz in range(-radius_cells, radius_cells + 1):
                    # 计算网格坐标
                    x = center_coords[0] + dx
                    y = center_coords[1] + dy
                    z = center_coords[2] + dz
                    
                    # 边界检查
                    if 0 <= x < self.grid_width and 0 <= y < self.grid_height and 0 <= z < self.grid_depth:
                        coords = (x, y, z)
                        
                        # 如果网格存在
                        if coords in self.grid:
                            cell = self.grid[coords]
                            
                            # 获取网格中的所有对象
                            for obj_id, (obj_position, obj_size) in cell.get_objects().items():
                                # 计算距离
                                distance = position.distance_to(obj_position)
                                
                                # 如果在查询范围内
                                if distance <= radius + obj_size:
                                    result_set.add(obj_id)
        
        return list(result_set)


class OptimizedCollisionDetector:
    """优化的碰撞检测器，用于检测车辆之间的碰撞"""
    
    def __init__(self, spatial_index: OptimizedSpatialIndex):
        """
        初始化碰撞检测器
        
        Args:
            spatial_index: 空间索引
        """
        self.spatial_index = spatial_index
        
        # 碰撞数据
        self.collisions = {}  # 对象ID -> 碰撞对象ID列表
        
        # 缓存
        self.nearby_objects_cache = {}  # 对象ID -> (时间戳, 附近对象列表)
        self.cache_timeout = 1.0  # 缓存超时时间（秒）
    
    def detect_collisions(self, object_id: str) -> List[str]:
        """
        检测对象的碰撞
        
        Args:
            object_id: 对象ID
            
        Returns:
            碰撞对象ID列表
        """
        # 获取对象信息
        obj_position = None
        obj_size = 0
        
        # 查找对象所在的网格
        if object_id in self.spatial_index.object_cells:
            coords_set = self.spatial_index.object_cells[object_id]
            if coords_set:
                # 获取第一个网格
                coords = next(iter(coords_set))
                if coords in self.spatial_index.grid:
                    cell = self.spatial_index.grid[coords]
                    if object_id in cell.objects:
                        obj_position, obj_size = cell.objects[object_id]
        
        if obj_position is None:
            return []
        
        # 检查缓存
        current_time = time.time()
        if object_id in self.nearby_objects_cache:
            cache_time, nearby_objects = self.nearby_objects_cache[object_id]
            if current_time - cache_time < self.cache_timeout:
                # 使用缓存的附近对象
                pass
            else:
                # 缓存过期，重新查询
                nearby_objects = self.spatial_index.query(obj_position, obj_size * 2)
                self.nearby_objects_cache[object_id] = (current_time, nearby_objects)
        else:
            # 没有缓存，查询附近的对象
            nearby_objects = self.spatial_index.query(obj_position, obj_size * 2)
            self.nearby_objects_cache[object_id] = (current_time, nearby_objects)
        
        # 过滤掉自身
        nearby_objects = [obj_id for obj_id in nearby_objects if obj_id != object_id]
        
        # 检测碰撞
        collisions = []
        for nearby_id in nearby_objects:
            # 获取附近对象信息
            nearby_position = None
            nearby_size = 0
            
            # 查找附近对象所在的网格
            if nearby_id in self.spatial_index.object_cells:
                coords_set = self.spatial_index.object_cells[nearby_id]
                if coords_set:
                    # 获取第一个网格
                    coords = next(iter(coords_set))
                    if coords in self.spatial_index.grid:
                        cell = self.spatial_index.grid[coords]
                        if nearby_id in cell.objects:
                            nearby_position, nearby_size = cell.objects[nearby_id]
            
            if nearby_position is None:
                continue
       <response clipped><NOTE>To save on context only part of this file has been shown to you. You should retry this tool after you have searched inside the file with `grep -n` in order to find the line numbers of what you are looking for.</NOTE>