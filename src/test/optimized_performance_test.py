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
from src.common.models import Position, Vector


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
            
            # 计算距离
            distance = obj_position.distance_to(nearby_position)
            
            # 如果距离小于两个对象的大小之和，则发生碰撞
            if distance < obj_size + nearby_size:
                collisions.append(nearby_id)
        
        # 更新碰撞数据
        self.collisions[object_id] = collisions
        
        return collisions


class OptimizedCollisionPredictionModel:
    """优化的碰撞预测模型，用于预测未来可能发生的碰撞"""
    
    def __init__(self, collision_detector: OptimizedCollisionDetector, prediction_time: float = 5.0):
        """
        初始化碰撞预测模型
        
        Args:
            collision_detector: 碰撞检测器
            prediction_time: 预测时间（秒）
        """
        self.collision_detector = collision_detector
        self.prediction_time = prediction_time
        
        # 预测数据
        self.predictions = {}  # 对象ID -> (预测碰撞对象ID, 预测碰撞时间)列表
        
        # 缓存
        self.prediction_cache = {}  # 对象ID -> (时间戳, 预测结果)
        self.cache_timeout = 1.0  # 缓存超时时间（秒）
    
    def predict_collisions(self, object_id: str) -> List[tuple]:
        """
        预测对象的碰撞
        
        Args:
            object_id: 对象ID
            
        Returns:
            预测碰撞列表 [(碰撞对象ID, 预测碰撞时间), ...]
        """
        # 检查缓存
        current_time = time.time()
        if object_id in self.prediction_cache:
            cache_time, predictions = self.prediction_cache[object_id]
            if current_time - cache_time < self.cache_timeout:
                # 使用缓存的预测结果
                return predictions
        
        # 获取对象信息
        obj_position = None
        obj_size = 0
        
        # 查找对象所在的网格
        if object_id in self.collision_detector.spatial_index.object_cells:
            coords_set = self.collision_detector.spatial_index.object_cells[object_id]
            if coords_set:
                # 获取第一个网格
                coords = next(iter(coords_set))
                if coords in self.collision_detector.spatial_index.grid:
                    cell = self.collision_detector.spatial_index.grid[coords]
                    if object_id in cell.objects:
                        obj_position, obj_size = cell.objects[object_id]
        
        if obj_position is None:
            return []
        
        # 查询附近的对象
        nearby_objects = self.collision_detector.spatial_index.query(obj_position, obj_size * 10)
        
        # 过滤掉自身
        nearby_objects = [obj_id for obj_id in nearby_objects if obj_id != object_id]
        
        # 预测碰撞
        predictions = []
        for nearby_id in nearby_objects:
            # 获取附近对象信息
            nearby_position = None
            nearby_size = 0
            
            # 查找附近对象所在的网格
            if nearby_id in self.collision_detector.spatial_index.object_cells:
                coords_set = self.collision_detector.spatial_index.object_cells[nearby_id]
                if coords_set:
                    # 获取第一个网格
                    coords = next(iter(coords_set))
                    if coords in self.collision_detector.spatial_index.grid:
                        cell = self.collision_detector.spatial_index.grid[coords]
                        if nearby_id in cell.objects:
                            nearby_position, nearby_size = cell.objects[nearby_id]
            
            if nearby_position is None:
                continue
            
            # 计算距离
            distance = obj_position.distance_to(nearby_position)
            
            # 如果距离已经小于两个对象的大小之和，则已经发生碰撞
            if distance < obj_size + nearby_size:
                continue
            
            # 预测碰撞时间
            collision_time = self._predict_collision_time(object_id, nearby_id, obj_position, obj_size, nearby_position, nearby_size)
            
            if collision_time is not None and collision_time <= self.prediction_time:
                predictions.append((nearby_id, collision_time))
        
        # 更新预测数据
        self.predictions[object_id] = predictions
        
        # 更新缓存
        self.prediction_cache[object_id] = (current_time, predictions)
        
        return predictions
    
    def _predict_collision_time(self, object_id1: str, object_id2: str, position1: Position, size1: float, position2: Position, size2: float) -> float:
        """
        预测两个对象的碰撞时间
        
        Args:
            object_id1: 对象1 ID
            object_id2: 对象2 ID
            position1: 对象1位置
            size1: 对象1大小
            position2: 对象2位置
            size2: 对象2大小
            
        Returns:
            预测碰撞时间（秒），如果不会碰撞则返回None
        """
        # 这里使用简化的碰撞预测模型
        # 实际应用中应该使用更复杂的物理模型
        
        # 计算距离
        distance = position1.distance_to(position2)
        
        # 如果距离已经小于两个对象的大小之和，则已经发生碰撞
        if distance < size1 + size2:
            return 0.0
        
        # 假设两个对象以恒定速度运动
        # 这里简化为使用随机速度
        relative_speed = random.uniform(5, 20)  # 5-20 m/s
        
        # 计算碰撞时间
        collision_time = (distance - size1 - size2) / relative_speed
        
        # 如果碰撞时间为负数，则不会碰撞
        if collision_time < 0:
            return None
        
        return collision_time


class ParallelCollisionDetection:
    """并行碰撞检测，使用多线程或多进程加速碰撞检测"""
    
    def __init__(self, spatial_index: OptimizedSpatialIndex, num_workers: int = None):
        """
        初始化并行碰撞检测
        
        Args:
            spatial_index: 空间索引
            num_workers: 工作线程数，默认为CPU核心数
        """
        self.spatial_index = spatial_index
        self.num_workers = num_workers or multiprocessing.cpu_count()
        
        # 创建碰撞检测器
        self.collision_detector = OptimizedCollisionDetector(spatial_index)
        
        # 创建碰撞预测模型
        self.prediction_model = OptimizedCollisionPredictionModel(self.collision_detector)
        
        # 结果
        self.collision_results = {}  # 对象ID -> 碰撞对象ID列表
        self.prediction_results = {}  # 对象ID -> (预测碰撞对象ID, 预测碰撞时间)列表
    
    def detect_collisions_batch(self, object_ids: List[str]) -> Dict[str, List[str]]:
        """
        批量检测碰撞
        
        Args:
            object_ids: 对象ID列表
            
        Returns:
            碰撞结果字典 {对象ID: 碰撞对象ID列表}
        """
        # 清空结果
        self.collision_results.clear()
        
        # 分割任务
        chunk_size = max(1, len(object_ids) // self.num_workers)
        chunks = [object_ids[i:i + chunk_size] for i in range(0, len(object_ids), chunk_size)]
        
        # 创建线程
        threads = []
        for chunk in chunks:
            thread = threading.Thread(target=self._detect_collisions_worker, args=(chunk,))
            threads.append(thread)
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        return self.collision_results
    
    def _detect_collisions_worker(self, object_ids: List[str]) -> None:
        """
        碰撞检测工作线程
        
        Args:
            object_ids: 对象ID列表
        """
        for object_id in object_ids:
            collisions = self.collision_detector.detect_collisions(object_id)
            self.collision_results[object_id] = collisions
    
    def predict_collisions_batch(self, object_ids: List[str]) -> Dict[str, List[tuple]]:
        """
        批量预测碰撞
        
        Args:
            object_ids: 对象ID列表
            
        Returns:
            预测结果字典 {对象ID: [(碰撞对象ID, 预测碰撞时间), ...]}
        """
        # 清空结果
        self.prediction_results.clear()
        
        # 分割任务
        chunk_size = max(1, len(object_ids) // self.num_workers)
        chunks = [object_ids[i:i + chunk_size] for i in range(0, len(object_ids), chunk_size)]
        
        # 创建线程
        threads = []
        for chunk in chunks:
            thread = threading.Thread(target=self._predict_collisions_worker, args=(chunk,))
            threads.append(thread)
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        return self.prediction_results
    
    def _predict_collisions_worker(self, object_ids: List[str]) -> None:
        """
        碰撞预测工作线程
        
        Args:
            object_ids: 对象ID列表
        """
        for object_id in object_ids:
            predictions = self.prediction_model.predict_collisions(object_id)
            self.prediction_results[object_id] = predictions


class OptimizedPerformanceTester:
    """优化的性能测试器，测试系统性能"""
    
    def __init__(self, 
                 num_vehicles: int = 1000,
                 map_size: tuple = (10000, 10000),
                 output_dir: str = "./results"):
        """
        初始化性能测试器
        
        Args:
            num_vehicles: 车辆数量
            map_size: 地图大小 (width, height)
            output_dir: 输出目录
        """
        self.num_vehicles = num_vehicles
        self.map_size = map_size
        self.output_dir = output_dir
        
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        # 车辆生成器
        self.vehicle_generator = VehicleGenerator(num_vehicles, map_size)
        
        # 空间索引
        self.spatial_index = OptimizedSpatialIndex(
            cell_size=100.0,
            map_size=(map_size[0], map_size[1], 100.0)
        )
        
        # 并行碰撞检测
        self.parallel_detector = ParallelCollisionDetection(
            spatial_index=self.spatial_index
        )
        
        # 性能指标
        self.latencies = []
        self.start_time = 0
        self.end_time = 0
        self.request_count = 0
        self.error_count = 0
        self.metrics_history = []
    
    def initialize(self) -> None:
        """初始化测试环境"""
        # 初始化车辆
        self.vehicle_generator.initialize_vehicles()
        
        # 重置指标
        self.latencies = []
        self.start_time = time.time()
        self.end_time = self.start_time
        self.request_count = 0
        self.error_count = 0
        self.metrics_history = []
    
    def run_test(self, duration: int = 60, target_tps: int = 1000) -> PerformanceMetrics:
        """
        运行性能测试
        
        Args:
            duration: 测试持续时间（秒）
            target_tps: 目标TPS
            
        Returns:
            性能指标
        """
        print(f"开始优化版性能测试，持续时间: {duration}秒，车辆数量: {self.num_vehicles}，目标TPS: {target_tps}")
        
        # 初始化测试环境
        self.initialize()
        
        # 计算请求间隔
        interval = 1.0 / target_tps
        
        # 爬升阶段
        ramp_up = min(duration * 0.2, 30)  # 最多30秒爬升时间
        print(f"爬升阶段: {ramp_up:.1f}秒")
        
        ramp_up_end_time = self.start_time + ramp_up
        while time.time() < ramp_up_end_time:
            # 更新车辆
            self.vehicle_generator.update_vehicles(interval)
            
            # 获取所有车辆
            vehicles = self.vehicle_generator.get_vehicles()
            
            # 执行碰撞检测
            start_time = time.time()
            try:
                # 更新空间索引
                self.spatial_index.clear()
                for vehicle in vehicles:
                    self.spatial_index.insert(vehicle.id, vehicle.position, vehicle.size)
                
                # 获取所有车辆ID
                vehicle_ids = [vehicle.id for vehicle in vehicles]
                
                # 并行检测碰撞
                self.parallel_detector.detect_collisions_batch(vehicle_ids)
                
                # 并行预测碰撞
                self.parallel_detector.predict_collisions_batch(vehicle_ids)
            except Exception as e:
                print(f"碰撞检测错误: {e}")
                self.error_count += 1
            
            end_time = time.time()
            latency = (end_time - start_time) * 1000  # 毫秒
            
            # 更新指标
            self.latencies.append(latency)
            self.request_count += 1
            
            # 计算当前指标
            if self.request_count % 10 == 0:
                metrics = self._calculate_metrics()
                self.metrics_history.append(metrics)
                
                # 打印当前指标
                elapsed = time.time() - self.start_time
                print(f"[{elapsed:.1f}s] 吞吐量: {metrics.throughput:.1f} req/s, "
                      f"平均延迟: {metrics.avg_latency:.2f} ms, "
                      f"P99延迟: {metrics.p99_latency:.2f} ms")
            
            # 等待下一个请求
            time.sleep(interval)
        
        # 持续阶段
        print(f"持续阶段: {duration - ramp_up:.1f}秒")
        steady_end_time = self.start_time + duration
        while time.time() < steady_end_time:
            # 更新车辆
            self.vehicle_generator.update_vehicles(interval)
            
            # 获取所有车辆
            vehicles = self.vehicle_generator.get_vehicles()
            
            # 执行碰撞检测
            start_time = time.time()
            try:
                # 更新空间索引
                self.spatial_index.clear()
                for vehicle in vehicles:
                    self.spatial_index.insert(vehicle.id, vehicle.position, vehicle.size)
                
                # 获取所有车辆ID
                vehicle_ids = [vehicle.id for vehicle in vehicles]
                
                # 并行检测碰撞
                self.parallel_detector.detect_collisions_batch(vehicle_ids)
                
                # 并行预测碰撞
                self.parallel_detector.predict_collisions_batch(vehicle_ids)
            except Exception as e:
                print(f"碰撞检测错误: {e}")
                self.error_count += 1
            
            end_time = time.time()
            latency = (end_time - start_time) * 1000  # 毫秒
            
            # 更新指标
            self.latencies.append(latency)
            self.request_count += 1
            
            # 计算当前指标
            if self.request_count % 10 == 0:
                metrics = self._calculate_metrics()
                self.metrics_history.append(metrics)
                
                # 打印当前指标
                elapsed = time.time() - self.start_time
                print(f"[{elapsed:.1f}s] 吞吐量: {metrics.throughput:.1f} req/s, "
                      f"平均延迟: {metrics.avg_latency:.2f} ms, "
                      f"P99延迟: {metrics.p99_latency:.2f} ms")
            
            # 等待下一个请求
            time.sleep(interval)
        
        self.end_time = time.time()
        
        # 计算最终指标
        final_metrics = self._calculate_metrics()
        
        # 保存结果
        self._save_results(target_tps, duration)
        
        return final_metrics
    
    def _calculate_metrics(self) -> PerformanceMetrics:
        """
        计算性能指标
        
        Returns:
            性能指标
        """
        # 计算吞吐量
        elapsed = max(0.001, time.time() - self.start_time)
        throughput = self.request_count / elapsed
        
        # 计算延迟
        if self.latencies:
            avg_latency = sum(self.latencies) / len(self.latencies)
            sorted_latencies = sorted(self.latencies)
            p95_index = int(len(sorted_latencies) * 0.95)
            p99_index = int(len(sorted_latencies) * 0.99)
            p95_latency = sorted_latencies[p95_index] if p95_index < len(sorted_latencies) else 0
            p99_latency = sorted_latencies[p99_index] if p99_index < len(sorted_latencies) else 0
            max_latency = max(self.latencies)
        else:
            avg_latency = 0
            p95_latency = 0
            p99_latency = 0
            max_latency = 0
        
        # 计算错误率
        error_rate = (self.error_count / max(1, self.request_count)) * 100
        
        # 获取系统资源使用情况
        try:
            import psutil
            cpu_usage = psutil.cpu_percent()
            memory_usage = psutil.virtual_memory().percent
        except ImportError:
            cpu_usage = 0
            memory_usage = 0
        
        return PerformanceMetrics(
            throughput=throughput,
            avg_latency=avg_latency,
            p95_latency=p95_latency,
            p99_latency=p99_latency,
            max_latency=max_latency,
            error_rate=error_rate,
            cpu_usage=cpu_usage,
            memory_usage=memory_usage,
            timestamp=time.time()
        )
    
    def _save_results(self, target_tps: int, duration: int) -> None:
        """
        保存测试结果
        
        Args:
            target_tps: 目标TPS
            duration: 测试持续时间
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_filename = f"{self.output_dir}/optimized_perf_test_{self.num_vehicles}vehicles_{target_tps}tps_{duration}s_{timestamp}"
        
        # 保存延迟数据
        with open(f"{base_filename}_latencies.csv", 'w') as f:
            f.write("latency_ms\n")
            for latency in self.latencies:
                f.write(f"{latency}\n")
        
        # 保存指标历史
        with open(f"{base_filename}_metrics.csv", 'w') as f:
            f.write("timestamp,throughput,avg_latency,p95_latency,p99_latency,max_latency,error_rate,cpu_usage,memory_usage\n")
            for metrics in self.metrics_history:
                f.write(f"{metrics.timestamp},{metrics.throughput},{metrics.avg_latency},{metrics.p95_latency},"
                        f"{metrics.p99_latency},{metrics.max_latency},{metrics.error_rate},{metrics.cpu_usage},{metrics.memory_usage}\n")
        
        # 生成摘要报告
        final_metrics = self._calculate_metrics()
        with open(f"{base_filename}_summary.txt", 'w') as f:
            f.write(f"优化版性能测试摘要\n")
            f.write(f"================\n\n")
            f.write(f"测试配置:\n")
            f.write(f"  车辆数量: {self.num_vehicles}\n")
            f.write(f"  目标TPS: {target_tps}\n")
            f.write(f"  持续时间: {duration}秒\n")
            f.write(f"  地图大小: {self.map_size}\n\n")
            
            f.write(f"测试结果:\n")
            f.write(f"  总请求数: {self.request_count}\n")
            f.write(f"  总错误数: {self.error_count}\n")
            f.write(f"  错误率: {final_metrics.error_rate:.2f}%\n")
            f.write(f"  实际持续时间: {self.end_time - self.start_time:.2f}秒\n")
            f.write(f"  吞吐量: {final_metrics.throughput:.2f}请求/秒\n\n")
            
            f.write(f"延迟 (毫秒):\n")
            f.write(f"  平均: {final_metrics.avg_latency:.2f}\n")
            f.write(f"  P95: {final_metrics.p95_latency:.2f}\n")
            f.write(f"  P99: {final_metrics.p99_latency:.2f}\n")
            f.write(f"  最大: {final_metrics.max_latency:.2f}\n\n")
            
            f.write(f"资源使用:\n")
            f.write(f"  CPU: {final_metrics.cpu_usage:.2f}%\n")
            f.write(f"  内存: {final_metrics.memory_usage:.2f}%\n")
        
        # 生成图表
        self._generate_charts(base_filename)
        
        print(f"结果保存到 {base_filename}_*")
    
    def _generate_charts(self, base_filename: str) -> None:
        """
        生成图表
        
        Args:
            base_filename: 基础文件名
        """
        # 提取数据
        timestamps = [(m.timestamp - self.start_time) for m in self.metrics_history]
        throughputs = [m.throughput for m in self.metrics_history]
        avg_latencies = [m.avg_latency for m in self.metrics_history]
        p95_latencies = [m.p95_latency for m in self.metrics_history]
        p99_latencies = [m.p99_latency for m in self.metrics_history]
        cpu_usages = [m.cpu_usage for m in self.metrics_history]
        memory_usages = [m.memory_usage for m in self.metrics_history]
        
        # 吞吐量图表
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps, throughputs)
        plt.title('吞吐量随时间变化')
        plt.xlabel('时间 (秒)')
        plt.ylabel('吞吐量 (请求/秒)')
        plt.grid(True)
        plt.savefig(f"{base_filename}_throughput.png")
        plt.close()
        
        # 延迟图表
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps, avg_latencies, label='平均')
        plt.plot(timestamps, p95_latencies, label='P95')
        plt.plot(timestamps, p99_latencies, label='P99')
        plt.title('延迟随时间变化')
        plt.xlabel('时间 (秒)')
        plt.ylabel('延迟 (毫秒)')
        plt.legend()
        plt.grid(True)
        plt.savefig(f"{base_filename}_latency.png")
        plt.close()
        
        # 资源使用图表
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps, cpu_usages, label='CPU')
        plt.plot(timestamps, memory_usages, label='内存')
        plt.title('资源使用随时间变化')
        plt.xlabel('时间 (秒)')
        plt.ylabel('使用率 (%)')
        plt.legend()
        plt.grid(True)
        plt.savefig(f"{base_filename}_resources.png")
        plt.close()
        
        # 延迟分布直方图
        plt.figure(figsize=(10, 6))
        plt.hist(self.latencies, bins=50)
        plt.title('延迟分布')
        plt.xlabel('延迟 (毫秒)')
        plt.ylabel('计数')
        plt.grid(True)
        plt.savefig(f"{base_filename}_latency_hist.png")
        plt.close()


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="优化版分布式实时计算平台性能测试")
    parser.add_argument("--vehicles", type=int, default=10000, help="车辆数量")
    parser.add_argument("--duration", type=int, default=60, help="测试持续时间（秒）")
    parser.add_argument("--tps", type=int, default=1000, help="目标TPS")
    parser.add_argument("--map-width", type=int, default=10000, help="地图宽度")
    parser.add_argument("--map-height", type=int, default=10000, help="地图高度")
    parser.add_argument("--output-dir", type=str, default="./results", help="输出目录")
    
    args = parser.parse_args()
    
    # 创建性能测试器
    tester = OptimizedPerformanceTester(
        num_vehicles=args.vehicles,
        map_size=(args.map_width, args.map_height),
        output_dir=args.output_dir
    )
    
    try:
        # 运行测试
        metrics = tester.run_test(
            duration=args.duration,
            target_tps=args.tps
        )
        
        # 打印结果摘要
        print("\n优化版测试结果摘要:")
        print(f"  吞吐量: {metrics.throughput:.2f} 请求/秒")
        print(f"  平均延迟: {metrics.avg_latency:.2f} ms")
        print(f"  P95延迟: {metrics.p95_latency:.2f} ms")
        print(f"  P99延迟: {metrics.p99_latency:.2f} ms")
        print(f"  错误率: {metrics.error_rate:.2f}%")
        print(f"  CPU使用率: {metrics.cpu_usage:.2f}%")
        print(f"  内存使用率: {metrics.memory_usage:.2f}%")
    except KeyboardInterrupt:
        print("测试被用户中断")
    except Exception as e:
        print(f"测试过程中出错: {e}")


if __name__ == "__main__":
    main()
