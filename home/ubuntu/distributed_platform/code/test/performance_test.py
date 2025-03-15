"""
简化版性能测试脚本，不依赖Kafka和Redis，直接测试系统核心组件性能
"""
import os
import sys
import time
import random
import json
import asyncio
import argparse
from datetime import datetime
from typing import Dict, List, Any
from dataclasses import dataclass, field

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


class SpatialIndex:
    """空间索引，用于快速查找空间中的对象"""
    
    def __init__(self, base_size: tuple = (10000, 10000, 100), min_size: tuple = (100, 100, 10), max_level: int = 3):
        """
        初始化空间索引
        
        Args:
            base_size: 基础网格大小 (width, height, depth)
            min_size: 最小网格大小 (width, height, depth)
            max_level: 最大网格层级
        """
        self.base_size = base_size
        self.min_size = min_size
        self.max_level = max_level
        
        # 网格数据
        self.grids = {}  # 网格ID -> 对象ID列表
        self.objects = {}  # 对象ID -> (位置, 大小)
        self.object_grids = {}  # 对象ID -> 网格ID列表
    
    def clear(self) -> None:
        """清空索引"""
        self.grids.clear()
        self.objects.clear()
        self.object_grids.clear()
    
    def insert(self, object_id: str, position: Position, size: float) -> None:
        """
        插入对象
        
        Args:
            object_id: 对象ID
            position: 对象位置
            size: 对象大小
        """
        # 存储对象信息
        self.objects[object_id] = (position, size)
        
        # 计算对象所在的网格
        grid_ids = self._get_grid_ids(position, size)
        
        # 存储对象所在的网格
        self.object_grids[object_id] = grid_ids
        
        # 更新网格中的对象列表
        for grid_id in grid_ids:
            if grid_id not in self.grids:
                self.grids[grid_id] = []
            
            self.grids[grid_id].append(object_id)
    
    def remove(self, object_id: str) -> None:
        """
        移除对象
        
        Args:
            object_id: 对象ID
        """
        if object_id not in self.object_grids:
            return
        
        # 获取对象所在的网格
        grid_ids = self.object_grids[object_id]
        
        # 从网格中移除对象
        for grid_id in grid_ids:
            if grid_id in self.grids and object_id in self.grids[grid_id]:
                self.grids[grid_id].remove(object_id)
        
        # 移除对象信息
        del self.object_grids[object_id]
        del self.objects[object_id]
    
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
        # 计算查询范围所在的网格
        grid_ids = self._get_grid_ids(position, radius)
        
        # 收集网格中的对象
        object_ids = set()
        for grid_id in grid_ids:
            if grid_id in self.grids:
                object_ids.update(self.grids[grid_id])
        
        # 过滤掉不在查询范围内的对象
        result = []
        for object_id in object_ids:
            if object_id in self.objects:
                obj_position, obj_size = self.objects[object_id]
                distance = position.distance_to(obj_position)
                if distance <= radius + obj_size:
                    result.append(object_id)
        
        return result
    
    def _get_grid_ids(self, position: Position, size: float) -> List[str]:
        """
        获取对象所在的网格ID
        
        Args:
            position: 对象位置
            size: 对象大小或查询半径
            
        Returns:
            网格ID列表
        """
        # 计算对象的边界框
        min_x = position.x - size
        max_x = position.x + size
        min_y = position.y - size
        max_y = position.y + size
        min_z = position.z - size
        max_z = position.z + size
        
        # 计算网格索引
        grid_ids = []
        
        # 计算基础网格索引
        base_grid_min_x = int(min_x / self.base_size[0])
        base_grid_max_x = int(max_x / self.base_size[0])
        base_grid_min_y = int(min_y / self.base_size[1])
        base_grid_max_y = int(max_y / self.base_size[1])
        base_grid_min_z = int(min_z / self.base_size[2])
        base_grid_max_z = int(max_z / self.base_size[2])
        
        # 收集基础网格
        for x in range(base_grid_min_x, base_grid_max_x + 1):
            for y in range(base_grid_min_y, base_grid_max_y + 1):
                for z in range(base_grid_min_z, base_grid_max_z + 1):
                    grid_id = f"{x},{y},{z}"
                    grid_ids.append(grid_id)
        
        return grid_ids


class SpatialPartitioner:
    """空间分区器，用于数据分片和负载均衡"""
    
    def __init__(self, spatial_index: SpatialIndex, num_shards: int = 10):
        """
        初始化空间分区器
        
        Args:
            spatial_index: 空间索引
            num_shards: 分片数量
        """
        self.spatial_index = spatial_index
        self.num_shards = num_shards
        
        # 分片数据
        self.grid_to_shard = {}  # 网格ID -> 分片ID
        self.shard_to_grids = {}  # 分片ID -> 网格ID列表
        self.shard_loads = {}  # 分片ID -> 负载
        
        # 初始化分片
        for i in range(num_shards):
            self.shard_to_grids[i] = []
            self.shard_loads[i] = 0
    
    def assign_grid(self, grid_id: str) -> int:
        """
        分配网格到分片
        
        Args:
            grid_id: 网格ID
            
        Returns:
            分片ID
        """
        if grid_id in self.grid_to_shard:
            return self.grid_to_shard[grid_id]
        
        # 找到负载最小的分片
        min_load = float('inf')
        min_shard = 0
        
        for shard_id, load in self.shard_loads.items():
            if load < min_load:
                min_load = load
                min_shard = shard_id
        
        # 分配网格到分片
        self.grid_to_shard[grid_id] = min_shard
        self.shard_to_grids[min_shard].append(grid_id)
        
        # 更新分片负载
        if grid_id in self.spatial_index.grids:
            self.shard_loads[min_shard] += len(self.spatial_index.grids[grid_id])
        
        return min_shard
    
    def get_shard(self, grid_id: str) -> int:
        """
        获取网格所在的分片
        
        Args:
            grid_id: 网格ID
            
        Returns:
            分片ID
        """
        if grid_id not in self.grid_to_shard:
            return self.assign_grid(grid_id)
        
        return self.grid_to_shard[grid_id]
    
    def get_shard_for_position(self, position: Position) -> int:
        """
        获取位置所在的分片
        
        Args:
            position: 位置
            
        Returns:
            分片ID
        """
        # 计算位置所在的网格
        x = int(position.x / self.spatial_index.base_size[0])
        y = int(position.y / self.spatial_index.base_size[1])
        z = int(position.z / self.spatial_index.base_size[2])
        
        grid_id = f"{x},{y},{z}"
        
        return self.get_shard(grid_id)
    
    def rebalance(self) -> None:
        """重新平衡分片负载"""
        # 计算平均负载
        total_load = sum(self.shard_loads.values())
        avg_load = total_load / self.num_shards
        
        # 找出负载过高和过低的分片
        high_shards = []
        low_shards = []
        
        for shard_id, load in self.shard_loads.items():
            if load > avg_load * 1.2:  # 负载超过平均值的120%
                high_shards.append((shard_id, load))
            elif load < avg_load * 0.8:  # 负载低于平均值的80%
                low_shards.append((shard_id, load))
        
        # 按负载排序
        high_shards.sort(key=lambda x: x[1], reverse=True)
        low_shards.sort(key=lambda x: x[1])
        
        # 从高负载分片移动网格到低负载分片
        for high_shard_id, high_load in high_shards:
            if not low_shards:
                break
            
            # 获取高负载分片的网格
            grids = self.shard_to_grids[high_shard_id]
            
            # 按网格负载排序
            grid_loads = []
            for grid_id in grids:
                if grid_id in self.spatial_index.grids:
                    grid_load = len(self.spatial_index.grids[grid_id])
                else:
                    grid_load = 0
                
                grid_loads.append((grid_id, grid_load))
            
            grid_loads.sort(key=lambda x: x[1], reverse=True)
            
            # 移动网格
            for grid_id, grid_load in grid_loads:
                if not low_shards:
                    break
                
                low_shard_id, low_load = low_shards[0]
                
                # 如果移动后高负载分片的负载仍然高于平均值，则继续移动
                if high_load - grid_load > avg_load:
                    # 从高负载分片移除网格
                    self.shard_to_grids[high_shard_id].remove(grid_id)
                    self.shard_loads[high_shard_id] -= grid_load
                    high_load -= grid_load
                    
                    # 添加到低负载分片
                    self.shard_to_grids[low_shard_id].append(grid_id)
                    self.shard_loads[low_shard_id] += grid_load
                    self.grid_to_shard[grid_id] = low_shard_id
                    
                    # 更新低负载分片
                    low_shards[0] = (low_shard_id, low_load + grid_load)
                    
                    # 重新排序低负载分片
                    low_shards.sort(key=lambda x: x[1])
                    
                    # 如果低负载分片的负载已经接近平均值，则移除
                    if low_shards[0][1] > avg_load * 0.9:
                        low_shards.pop(0)
                else:
                    break


class CollisionDetector:
    """碰撞检测器，用于检测车辆之间的碰撞"""
    
    def __init__(self, spatial_index: SpatialIndex):
        """
        初始化碰撞检测器
        
        Args:
            spatial_index: 空间索引
        """
        self.spatial_index = s<response clipped><NOTE>To save on context only part of this file has been shown to you. You should retry this tool after you have searched inside the file with `grep -n` in order to find the line numbers of what you are looking for.</NOTE>