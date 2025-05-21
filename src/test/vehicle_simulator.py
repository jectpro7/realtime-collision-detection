"""
车辆位置模拟器，用于生成模拟车辆位置数据。
支持不同的车辆分布模式和移动模式。
"""
import time
import uuid
import random
import math
import json
import argparse
import asyncio
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field

import numpy as np
from kafka import KafkaProducer
import redis


@dataclass
class Position:
    """位置"""
    x: float
    y: float
    z: float


@dataclass
class Vector:
    """向量"""
    x: float
    y: float
    z: float


@dataclass
class Vehicle:
    """车辆"""
    id: str
    position: Position
    velocity: Vector
    acceleration: Vector
    heading: float
    size: float
    type: str
    timestamp: float


@dataclass
class RoadSegment:
    """道路段"""
    id: str
    start: Position
    end: Position
    width: float
    speed_limit: float
    connections: List[str] = field(default_factory=list)


@dataclass
class City:
    """城市"""
    id: str
    center: Position
    radius: float
    density: float  # 车辆密度因子


class TrafficMap:
    """交通地图，包含道路网络和城市"""
    
    def __init__(self, size: Tuple[float, float] = (10000.0, 10000.0)):
        """
        初始化交通地图
        
        Args:
            size: 地图大小 (width, height)
        """
        self.size = size
        self.roads: Dict[str, RoadSegment] = {}
        self.cities: Dict[str, City] = {}
        self.intersections: Dict[str, Position] = {}
    
    def add_road(self, road: RoadSegment) -> None:
        """
        添加道路
        
        Args:
            road: 道路段
        """
        self.roads[road.id] = road
    
    def add_city(self, city: City) -> None:
        """
        添加城市
        
        Args:
            city: 城市
        """
        self.cities[city.id] = city
    
    def add_intersection(self, intersection_id: str, position: Position) -> None:
        """
        添加交叉路口
        
        Args:
            intersection_id: 交叉路口ID
            position: 位置
        """
        self.intersections[intersection_id] = position
    
    def connect_roads(self, road1_id: str, road2_id: str) -> None:
        """
        连接两条道路
        
        Args:
            road1_id: 道路1 ID
            road2_id: 道路2 ID
        """
        if road1_id in self.roads and road2_id in self.roads:
            self.roads[road1_id].connections.append(road2_id)
            self.roads[road2_id].connections.append(road1_id)
    
    def get_random_position(self) -> Position:
        """
        获取随机位置
        
        Returns:
            随机位置
        """
        return Position(
            x=random.uniform(0, self.size[0]),
            y=random.uniform(0, self.size[1]),
            z=0.0
        )
    
    def get_position_on_road(self, road_id: Optional[str] = None) -> Tuple[Position, float, str]:
        """
        获取道路上的随机位置
        
        Args:
            road_id: 道路ID，如果为None则随机选择
            
        Returns:
            (位置, 航向角, 道路ID)
        """
        if not self.roads:
            return self.get_random_position(), 0.0, ""
        
        # 如果未指定道路，随机选择一条
        if road_id is None or road_id not in self.roads:
            road_id = random.choice(list(self.roads.keys()))
        
        road = self.roads[road_id]
        
        # 在道路上随机选择一个点
        t = random.random()  # 0-1之间的参数
        pos = Position(
            x=road.start.x + t * (road.end.x - road.start.x),
            y=road.start.y + t * (road.end.y - road.start.y),
            z=road.start.z + t * (road.end.z - road.start.z)
        )
        
        # 计算航向角（道路方向）
        dx = road.end.x - road.start.x
        dy = road.end.y - road.start.y
        heading = math.atan2(dy, dx)
        
        return pos, heading, road_id
    
    def get_position_near_city(self, city_id: Optional[str] = None) -> Position:
        """
        获取城市附近的随机位置
        
        Args:
            city_id: 城市ID，如果为None则随机选择
            
        Returns:
            城市附近的随机位置
        """
        if not self.cities:
            return self.get_random_position()
        
        # 如果未指定城市，随机选择一个
        if city_id is None or city_id not in self.cities:
            city_id = random.choice(list(self.cities.keys()))
        
        city = self.cities[city_id]
        
        # 在城市范围内随机选择一个点
        r = random.random() * city.radius
        theta = random.random() * 2 * math.pi
        
        return Position(
            x=city.center.x + r * math.cos(theta),
            y=city.center.y + r * math.sin(theta),
            z=city.center.z
        )
    
    def get_next_road(self, current_road_id: str) -> Optional[str]:
        """
        获取下一条连接的道路
        
        Args:
            current_road_id: 当前道路ID
            
        Returns:
            下一条道路ID，如果没有连接的道路则返回None
        """
        if current_road_id not in self.roads:
            return None
        
        connections = self.roads[current_road_id].connections
        if not connections:
            return None
        
        return random.choice(connections)
    
    def generate_grid_map(self, width: int, height: int, cell_size: float = 100.0) -> None:
        """
        生成网格地图
        
        Args:
            width: 网格宽度（单元格数量）
            height: 网格高度（单元格数量）
            cell_size: 单元格大小（米）
        """
        # 设置地图大小
        self.size = (width * cell_size, height * cell_size)
        
        # 生成水平道路
        for i in range(height + 1):
            road_id = f"h-road-{i}"
            start = Position(x=0, y=i * cell_size, z=0)
            end = Position(x=width * cell_size, y=i * cell_size, z=0)
            road = RoadSegment(id=road_id, start=start, end=end, width=5.0, speed_limit=13.9)  # 50 km/h
            self.add_road(road)
        
        # 生成垂直道路
        for i in range(width + 1):
            road_id = f"v-road-{i}"
            start = Position(x=i * cell_size, y=0, z=0)
            end = Position(x=i * cell_size, y=height * cell_size, z=0)
            road = RoadSegment(id=road_id, start=start, end=end, width=5.0, speed_limit=13.9)  # 50 km/h
            self.add_road(road)
        
        # 添加交叉路口并连接道路
        for i in range(width + 1):
            for j in range(height + 1):
                intersection_id = f"intersection-{i}-{j}"
                position = Position(x=i * cell_size, y=j * cell_size, z=0)
                self.add_intersection(intersection_id, position)
                
                # 连接相交的道路
                if i < width:
                    self.connect_roads(f"h-road-{j}", f"v-road-{i}")
                if j < height:
                    self.connect_roads(f"h-road-{j}", f"v-road-{i+1}")
        
        # 添加城市（在一些交叉路口）
        num_cities = min(5, (width + 1) * (height + 1) // 4)
        for _ in range(num_cities):
            i = random.randint(0, width)
            j = random.randint(0, height)
            city_id = f"city-{i}-{j}"
            position = Position(x=i * cell_size, y=j * cell_size, z=0)
            radius = random.uniform(cell_size * 1.5, cell_size * 3)
            density = random.uniform(0.5, 1.0)
            city = City(id=city_id, center=position, radius=radius, density=density)
            self.add_city(city)
    
    def generate_random_map(self, num_roads: int = 50, num_cities: int = 5) -> None:
        """
        生成随机地图
        
        Args:
            num_roads: 道路数量
            num_cities: 城市数量
        """
        # 生成随机道路
        for i in range(num_roads):
            road_id = f"road-{i}"
            start = self.get_random_position()
            end = self.get_random_position()
            road = RoadSegment(id=road_id, start=start, end=end, width=5.0, speed_limit=13.9)  # 50 km/h
            self.add_road(road)
        
        # 随机连接一些道路
        for _ in range(num_roads * 2):
            road1_id = f"road-{random.randint(0, num_roads-1)}"
            road2_id = f"road-{random.randint(0, num_roads-1)}"
            if road1_id != road2_id:
                self.connect_roads(road1_id, road2_id)
        
        # 生成随机城市
        for i in range(num_cities):
            city_id = f"city-{i}"
            position = self.get_random_position()
            radius = random.uniform(500, 2000)
            density = random.uniform(0.5, 1.0)
            city = City(id=city_id, center=position, radius=radius, density=density)
            self.add_city(city)


class VehicleSimulator:
    """车辆模拟器，生成模拟车辆数据"""
    
    def __init__(self, traffic_map: TrafficMap, num_vehicles: int = 1000):
        """
        初始化车辆模拟器
        
        Args:
            traffic_map: 交通地图
            num_vehicles: 车辆数量
        """
        self.traffic_map = traffic_map
        self.num_vehicles = num_vehicles
        self.vehicles: Dict[str, Vehicle] = {}
        self.vehicle_roads: Dict[str, str] = {}  # vehicle_id -> road_id
        self.vehicle_targets: Dict[str, Position] = {}  # vehicle_id -> target_position
        
        # 车辆类型
        self.vehicle_types = ["car", "truck", "bus", "motorcycle"]
        self.vehicle_sizes = {"car": 2.0, "truck": 4.0, "bus": 5.0, "motorcycle": 1.0}
        
        # 移动模式
        self.movement_modes = ["random", "road_constrained", "destination_oriented"]
        self.vehicle_modes: Dict[str, str] = {}  # vehicle_id -> movement_mode
        
        # 分布模式
        self.distribution_modes = ["uniform", "city_centered"]
        
        # 统计信息
        self.stats = {
            "total_vehicles": 0,
            "active_vehicles": 0,
            "city_vehicles": 0,
            "road_vehicles": 0,
            "updates_per_second": 0
        }
    
    def initialize_vehicles(self, distribution_mode: str = "city_centered") -> None:
        """
        初始化车辆
        
        Args:
            distribution_mode: 分布模式 ("uniform" 或 "city_centered")
        """
        self.vehicles.clear()
        self.vehicle_roads.clear()
        self.vehicle_targets.clear()
        self.vehicle_modes.clear()
        
        for i in range(self.num_vehicles):
            vehicle_id = f"vehicle-{i}"
            
            # 确定车辆类型
            vehicle_type = random.choice(self.vehicle_types)
            vehicle_size = self.vehicle_sizes[vehicle_type]
            
            # 确定移动模式
            movement_mode = random.choice(self.movement_modes)
            self.vehicle_modes[vehicle_id] = movement_mode
            
            # 根据分布模式确定初始位置
            if distribution_mode == "uniform":
                # 均匀分布
                if movement_mode == "road_constrained":
                    # 道路约束移动，初始位置在道路上
                    position, heading, road_id = self.traffic_map.get_position_on_road()
                    self.vehicle_roads[vehicle_id] = road_id
                else:
                    # 其他移动模式，初始位置随机
                    position = self.traffic_map.get_random_position()
                    heading = random.uniform(0, 2 * math.pi)
            else:
                # 城市集中分布
                # 80%的车辆在城市附近，20%均匀分布
                if random.random() < 0.8:
                    position = self.traffic_map.get_position_near_city()
                    self.stats["city_vehicles"] += 1
                    heading = random.uniform(0, 2 * math.pi)
                else:
                    if movement_mode == "road_constrained":
                        position, heading, road_id = self.traffic_map.get_position_on_road()
                        self.vehicle_roads[vehicle_id] = road_id
                        self.stats["road_vehicles"] += 1
                    else:
                        position = self.traffic_map.get_random_position()
                        heading = random.uniform(0, 2 * math.pi)
            
            # 初始速度和加速度
            if movement_mode == "road_constrained" and vehicle_id in self.vehicle_roads:
                road_id = self.vehicle_roads[vehicle_id]
                speed_limit = self.traffic_map.roads[road_id].speed_limit
                speed = random.uniform(speed_limit * 0.7, speed_limit)
            else:
                speed = random.uniform(5, 20)  # 5-20 m/s (18-72 km/h)
            
            velocity = Vector(
                x=speed * math.cos(heading),
                y=speed * math.sin(heading),
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
            
            # 对于目的地导向移动，设置目标位置
            if movement_mode == "destination_oriented":
                if random.random() < 0.7:
                    # 70%的目标是城市
                    target_position = self.traffic_map.get_position_near_city()
                else:
                    # 30%的目标是随机位置
                    target_position = self.traffic_map.get_random_position()
                
                self.vehicle_targets[vehicle_id] = target_position
        
        self.stats["total_vehicles"] = len(self.vehicles)
        self.stats["active_vehicles"] = len(self.vehicles)
    
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
            # 根据移动模式更新位置
            movement_mode = self.vehicle_modes.get(vehicle_id, "random")
            
            if movement_mode == "random":
                self._update_random_movement(vehicle, time_delta)
            elif movement_mode == "road_constrained":
                self._update_road_movement(vehicle, vehicle_id, time_delta)
            elif movement_mode == "destination_oriented":
                self._update_destination_movement(vehicle, vehicle_id, time_delta)
            
            # 更新时间戳
            vehicle.timestamp = time.time()
            
            # 添加到更新列表
            updated_vehicles.append(vehicle)
        
        self.stats["updates_per_second"] = len(updated_vehicles) / time_delta
        return updated_vehicles
    
    def _update_random_movement(self, vehicle: Vehicle, time_delta: float) -> None:
        """
        随机移动更新
        
        Args:
            vehicle: 车辆对象
            time_delta: 时间增量（秒）
        """
        # 随机改变加速度
        if random.random() < 0.1:
            vehicle.acceleration.x = random.uniform(-1, 1)
            vehicle.acceleration.y = random.uniform(-1, 1)
        
        # 更新速度
        vehicle.velocity.x += vehicle.acceleration.x * time_delta
        vehicle.velocity.y += vehicle.acceleration.y * time_delta
        
        # 限制速度
        speed = math.sqrt(vehicle.velocity.x**2 + vehicle.velocity.y**2)
        max_speed = 30.0  # 最大速度 30 m/s (108 km/h)
        if speed > max_speed:
            vehicle.velocity.x = vehicle.velocity.x / speed * max_speed
            vehicle.velocity.y = vehicle.velocity.y / speed * max_speed
        
        # 更新位置
        vehicle.position.x += vehicle.velocity.x * time_delta
        vehicle.position.y += vehicle.velocity.y * time_delta
        
        # 更新航向角
        if speed > 0.1:
            vehicle.heading = math.atan2(vehicle.velocity.y, vehicle.velocity.x)
        
        # 边界检查
        map_width, map_height = self.traffic_map.size
        if vehicle.position.x < 0:
            vehicle.position.x = 0
            vehicle.velocity.x = -vehicle.velocity.x * 0.5
        elif vehicle.position.x > map_width:
            vehicle.position.x = map_width
            vehicle.velocity.x = -vehicle.velocity.x * 0.5
        
        if vehicle.position.y < 0:
            vehicle.position.y = 0
            vehicle.velocity.y = -vehicle.velocity.y * 0.5
        elif vehicle.position.y > map_height:
            vehicle.position.y = map_height
            vehicle.velocity.y = -vehicle.velocity.y * 0.5
    
    def _update_road_movement(self, vehicle: Vehicle, vehicle_id: str, time_delta: float) -> None:
        """
        道路约束移动更新
        
        Args:
            vehicle: 车辆对象
            vehicle_id: 车辆ID
            time_delta: 时间增量（秒）
        """
        # 获取当前道路
        road_id = self.vehicle_roads.get(vehicle_id)
        if not road_id or road_id not in self.traffic_map.roads:
            # 如果没有道路，分配一条
            _, _, road_id = self.traffic_map.get_position_on_road()
            self.vehicle_roads[vehicle_id] = road_id
        
        road = self.traffic_map.roads[road_id]
        
        # 计算道路方向
        dx = road.end.x - road.start.x
        dy = road.end.y - road.start.y
        road_length = math.sqrt(dx**2 + dy**2)
        
        if road_length < 0.1:
            return
        
        road_direction_x = dx / road_length
        road_direction_y = dy / road_length
        
        # 计算车辆在道路上的投影位置
        vehicle_dx = vehicle.position.x - road.start.x
        vehicle_dy = vehicle.position.y - road.start.y
        
        # 计算沿道路方向的距离
        distance_along_road = vehicle_dx * road_direction_x + vehicle_dy * road_direction_y
        
        # 检查是否到达道路尽头
        if distance_along_road >= road_length:
            # 切换到下一条道路
            next_road_id = self.traffic_map.get_next_road(road_id)
            if next_road_id:
                self.vehicle_roads[vehicle_id] = next_road_id
                # 将车辆放在新道路的起点
                new_road = self.traffic_map.roads[next_road_id]
                vehicle.position.x = new_road.start.x
                vehicle.position.y = new_road.start.y
                
                # 计算新道路方向
                new_dx = new_road.end.x - new_road.start.x
                new_dy = new_road.end.y - new_road.start.y
                new_road_length = math.sqrt(new_dx**2 + new_dy**2)
                
                if new_road_length > 0.1:
                    new_direction_x = new_dx / new_road_length
                    new_direction_y = new_dy / new_road_length
                    
                    # 更新航向角
                    vehicle.heading = math.atan2(new_direction_y, new_direction_x)
                    
                    # 更新速度
                    speed = math.sqrt(vehicle.velocity.x**2 + vehicle.velocity.y**2)
                    vehicle.velocity.x = speed * new_direction_x
                    vehicle.velocity.y = speed * new_direction_y
            else:
                # 如果没有下一条道路，反向行驶
                vehicle.position.x = road.end.x
                vehicle.position.y = road.end.y
                vehicle.velocity.x = -vehicle.velocity.x
                vehicle.velocity.y = -vehicle.velocity.y
                vehicle.heading = math.atan2(vehicle.velocity.y, vehicle.velocity.x)
        elif distance_along_road < 0:
            # 如果在道路起点之前，反向行驶
            vehicle.position.x = road.start.x
            vehicle.position.y = road.start.y
            vehicle.velocity.x = -vehicle.velocity.x
            vehicle.velocity.y = -vehicle.velocity.y
            vehicle.heading = math.atan2(vehicle.velocity.y, vehicle.velocity.x)
        else:
            # 正常沿道路行驶
            # 随机改变加速度
            if random.random() < 0.1:
                vehicle.acceleration.x = random.uniform(-1, 1) * road_direction_x
                vehicle.acceleration.y = random.uniform(-1, 1) * road_direction_y
            
            # 更新速度
            vehicle.velocity.x += vehicle.acceleration.x * time_delta
            vehicle.velocity.y += vehicle.acceleration.y * time_delta
            
            # 限制速度
            speed = math.sqrt(vehicle.velocity.x**2 + vehicle.velocity.y**2)
            max_speed = road.speed_limit
            if speed > max_speed:
                vehicle.velocity.x = vehicle.velocity.x / speed * max_speed
                vehicle.velocity.y = vehicle.velocity.y / speed * max_speed
            
            # 确保车辆沿道路方向行驶
            dot_product = vehicle.velocity.x * road_direction_x + vehicle.velocity.y * road_direction_y
            if dot_product < 0:
                # 如果车辆反向行驶，调整方向
                vehicle.velocity.x = abs(speed) * road_direction_x
                vehicle.velocity.y = abs(speed) * road_direction_y
            
            # 更新位置
            vehicle.position.x += vehicle.velocity.x * time_delta
            vehicle.position.y += vehicle.velocity.y * time_delta
            
            # 更新航向角
            vehicle.heading = math.atan2(vehicle.velocity.y, vehicle.velocity.x)
    
    def _update_destination_movement(self, vehicle: Vehicle, vehicle_id: str, time_delta: float) -> None:
        """
        目的地导向移动更新
        
        Args:
            vehicle: 车辆对象
            vehicle_id: 车辆ID
            time_delta: 时间增量（秒）
        """
        # 获取目标位置
        target_position = self.vehicle_targets.get(vehicle_id)
        if not target_position:
            # 如果没有目标，设置一个
            if random.random() < 0.7:
                # 70%的目标是城市
                target_position = self.traffic_map.get_position_near_city()
            else:
                # 30%的目标是随机位置
                target_position = self.traffic_map.get_random_position()
            
            self.vehicle_targets[vehicle_id] = target_position
        
        # 计算到目标的方向
        dx = target_position.x - vehicle.position.x
        dy = target_position.y - vehicle.position.y
        distance = math.sqrt(dx**2 + dy**2)
        
        # 检查是否到达目标
        if distance < 10.0:
            # 设置新目标
            if random.random() < 0.7:
                # 70%的目标是城市
                target_position = self.traffic_map.get_position_near_city()
            else:
                # 30%的目标是随机位置
                target_position = self.traffic_map.get_random_position()
            
            self.vehicle_targets[vehicle_id] = target_position
            
            # 重新计算方向
            dx = target_position.x - vehicle.position.x
            dy = target_position.y - vehicle.position.y
            distance = math.sqrt(dx**2 + dy**2)
        
        if distance > 0.1:
            direction_x = dx / distance
            direction_y = dy / distance
            
            # 计算期望速度
            desired_speed = min(20.0, distance / 2)  # 最大20 m/s，接近目标时减速
            
            # 计算当前速度
            current_speed = math.sqrt(vehicle.velocity.x**2 + vehicle.velocity.y**2)
            
            # 计算加速度
            if desired_speed > current_speed:
                # 加速
                acceleration = 2.0
            else:
                # 减速
                acceleration = -2.0
            
            # 更新速度
            vehicle.velocity.x += acceleration * direction_x * time_delta
            vehicle.velocity.y += acceleration * direction_y * time_delta
            
            # 限制速度
            speed = math.sqrt(vehicle.velocity.x**2 + vehicle.velocity.y**2)
            max_speed = 30.0  # 最大速度 30 m/s (108 km/h)
            if speed > max_speed:
                vehicle.velocity.x = vehicle.velocity.x / speed * max_speed
                vehicle.velocity.y = vehicle.velocity.y / speed * max_speed
            
            # 更新位置
            vehicle.position.x += vehicle.velocity.x * time_delta
            vehicle.position.y += vehicle.velocity.y * time_delta
            
            # 更新航向角
            vehicle.heading = math.atan2(vehicle.velocity.y, vehicle.velocity.x)
            
            # 更新加速度
            vehicle.acceleration.x = acceleration * direction_x
            vehicle.acceleration.y = acceleration * direction_y
        
        # 边界检查
        map_width, map_height = self.traffic_map.size
        if vehicle.position.x < 0:
            vehicle.position.x = 0
        elif vehicle.position.x > map_width:
            vehicle.position.x = map_width
        
        if vehicle.position.y < 0:
            vehicle.position.y = 0
        elif vehicle.position.y > map_height:
            vehicle.position.y = map_height
    
    def get_vehicle_json(self, vehicle: Vehicle) -> str:
        """
        获取车辆JSON表示
        
        Args:
            vehicle: 车辆对象
            
        Returns:
            JSON字符串
        """
        return json.dumps({
            "id": vehicle.id,
            "position": {
                "x": vehicle.position.x,
                "y": vehicle.position.y,
                "z": vehicle.position.z
            },
            "velocity": {
                "x": vehicle.velocity.x,
                "y": vehicle.velocity.y,
                "z": vehicle.velocity.z
            },
            "acceleration": {
                "x": vehicle.acceleration.x,
                "y": vehicle.acceleration.y,
                "z": vehicle.acceleration.z
            },
            "heading": vehicle.heading,
            "size": vehicle.size,
            "type": vehicle.type,
            "timestamp": vehicle.timestamp
        })
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取统计信息
        
        Returns:
            统计信息字典
        """
        return self.stats


class KafkaVehicleProducer:
    """Kafka车辆数据生产者"""
    
    def __init__(self, bootstrap_servers: str, topic: str):
        """
        初始化Kafka生产者
        
        Args:
            bootstrap_servers: Kafka服务器地址
            topic: 主题
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: v.encode('utf-8')
        )
    
    def send_vehicle_data(self, vehicle_json: str) -> None:
        """
        发送车辆数据
        
        Args:
            vehicle_json: 车辆JSON数据
        """
        self.producer.send(self.topic, vehicle_json)
    
    def close(self) -> None:
        """关闭生产者"""
        self.producer.flush()
        self.producer.close()


class RedisVehicleProducer:
    """Redis车辆数据生产者"""
    
    def __init__(self, host: str, port: int, channel: str):
        """
        初始化Redis生产者
        
        Args:
            host: Redis主机
            port: Redis端口
            channel: 发布通道
        """
        self.host = host
        self.port = port
        self.channel = channel
        self.redis_client = redis.Redis(host=host, port=port)
    
    def send_vehicle_data(self, vehicle_json: str) -> None:
        """
        发送车辆数据
        
        Args:
            vehicle_json: 车辆JSON数据
        """
        self.redis_client.publish(self.channel, vehicle_json)
    
    def close(self) -> None:
        """关闭生产者"""
        self.redis_client.close()


async def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="Vehicle Position Simulator")
    parser.add_argument("--num-vehicles", type=int, default=1000, help="Number of vehicles to simulate")
    parser.add_argument("--update-rate", type=float, default=1.0, help="Update rate in seconds")
    parser.add_argument("--distribution", type=str, default="city_centered", choices=["uniform", "city_centered"], help="Vehicle distribution mode")
    parser.add_argument("--output", type=str, default="kafka", choices=["kafka", "redis", "console"], help="Output mode")
    parser.add_argument("--kafka-servers", type=str, default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--kafka-topic", type=str, default="vehicle-positions", help="Kafka topic")
    parser.add_argument("--redis-host", type=str, default="localhost", help="Redis host")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis port")
    parser.add_argument("--redis-channel", type=str, default="vehicle-positions", help="Redis channel")
    parser.add_argument("--map-type", type=str, default="grid", choices=["grid", "random"], help="Map type")
    parser.add_argument("--map-size", type=int, default=10, help="Map size (grid cells)")
    parser.add_argument("--run-time", type=int, default=0, help="Run time in seconds (0 for infinite)")
    args = parser.parse_args()
    
    # 创建交通地图
    traffic_map = TrafficMap()
    if args.map_type == "grid":
        traffic_map.generate_grid_map(args.map_size, args.map_size)
    else:
        traffic_map.generate_random_map()
    
    # 创建车辆模拟器
    simulator = VehicleSimulator(traffic_map, args.num_vehicles)
    simulator.initialize_vehicles(args.distribution)
    
    # 创建数据生产者
    producer = None
    if args.output == "kafka":
        producer = KafkaVehicleProducer(args.kafka_servers, args.kafka_topic)
    elif args.output == "redis":
        producer = RedisVehicleProducer(args.redis_host, args.redis_port, args.redis_channel)
    
    # 运行模拟
    start_time = time.time()
    update_count = 0
    try:
        while True:
            # 更新车辆
            vehicles = simulator.update_vehicles(args.update_rate)
            update_count += 1
            
            # 发送数据
            for vehicle in vehicles:
                vehicle_json = simulator.get_vehicle_json(vehicle)
                
                if args.output == "console":
                    print(vehicle_json)
                elif producer:
                    producer.send_vehicle_data(vehicle_json)
            
            # 打印统计信息
            if update_count % 10 == 0:
                stats = simulator.get_stats()
                print(f"Update {update_count}: {stats['active_vehicles']} vehicles, "
                      f"{stats['updates_per_second']:.2f} updates/s")
            
            # 检查运行时间
            if args.run_time > 0 and time.time() - start_time > args.run_time:
                break
            
            # 等待下一次更新
            await asyncio.sleep(args.update_rate)
    except KeyboardInterrupt:
        print("Simulation stopped by user")
    finally:
        if producer:
            producer.close()
        
        # 打印最终统计信息
        elapsed_time = time.time() - start_time
        total_updates = update_count * args.num_vehicles
        print(f"\nSimulation summary:")
        print(f"  Run time: {elapsed_time:.2f} seconds")
        print(f"  Updates: {update_count}")
        print(f"  Vehicles: {args.num_vehicles}")
        print(f"  Total vehicle updates: {total_updates}")
        print(f"  Average update rate: {total_updates / elapsed_time:.2f} vehicle updates/s")


if __name__ == "__main__":
    asyncio.run(main())
