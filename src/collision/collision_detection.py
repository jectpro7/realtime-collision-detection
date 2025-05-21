"""
碰撞检测算法实现，用于高效检测车辆间潜在碰撞。
基于多阶段碰撞检测设计。
"""
import time
import math
import uuid
import numpy as np
from typing import Dict, List, Set, Tuple, Optional, Any
from dataclasses import dataclass, field

from ..common.models import Position, Vector, Vehicle, CollisionRisk
from ..common.utils import get_logger, Timer
from .spatial_index import SpatialIndex

logger = get_logger(__name__)

# 常量定义
SAFE_DISTANCE_DEFAULT = 5.0  # 默认安全距离（米）
MAX_WARNING_TIME = 10.0  # 最大预警时间（秒）
MAX_RELATIVE_SPEED = 50.0  # 最大相对速度（米/秒）

# 风险评估权重
WEIGHT_DISTANCE = 0.3
WEIGHT_TIME = 0.3
WEIGHT_SPEED = 0.2
WEIGHT_ANGLE = 0.1
WEIGHT_TYPE = 0.1


@dataclass
class CollisionInfo:
    """碰撞信息"""
    vehicle_id1: str  # 车辆1 ID
    vehicle_id2: str  # 车辆2 ID
    collision_time: float  # 碰撞时间（秒）
    collision_position: Position  # 碰撞位置
    distance: float  # 当前距离
    safe_distance: float  # 安全距离
    relative_speed: float  # 相对速度
    risk_level: float  # 风险等级（0-1）
    timestamp: float = field(default_factory=time.time)  # 检测时间戳


class CollisionDetector:
    """碰撞检测器实现，基于多阶段碰撞检测设计"""
    
    def __init__(self, spatial_index: SpatialIndex):
        """
        初始化碰撞检测器
        
        Args:
            spatial_index: 空间索引
        """
        self.spatial_index = spatial_index
        
        # 车辆状态缓存
        self.vehicle_cache: Dict[str, Vehicle] = {}
        
        # 碰撞风险缓存
        self.collision_risks: Dict[str, Dict[str, CollisionRisk]] = {}  # vehicle_id -> {other_id -> risk}
        
        # 统计信息
        self.stats = {
            "total_detections": 0,
            "potential_collisions": 0,
            "high_risk_collisions": 0,
            "avg_detection_time_ms": 0.0,
            "max_detection_time_ms": 0.0
        }
        
        logger.info("Collision detector initialized")
    
    def update_vehicle(self, vehicle: Vehicle) -> None:
        """
        更新车辆状态
        
        Args:
            vehicle: 车辆对象
        """
        # 更新车辆缓存
        self.vehicle_cache[vehicle.id] = vehicle
        
        # 更新空间索引
        self.spatial_index.insert_vehicle(vehicle.id, vehicle.position)
    
    def remove_vehicle(self, vehicle_id: str) -> None:
        """
        移除车辆
        
        Args:
            vehicle_id: 车辆ID
        """
        # 从车辆缓存中移除
        if vehicle_id in self.vehicle_cache:
            del self.vehicle_cache[vehicle_id]
        
        # 从空间索引中移除
        self.spatial_index.remove_vehicle(vehicle_id)
        
        # 从碰撞风险缓存中移除
        if vehicle_id in self.collision_risks:
            del self.collision_risks[vehicle_id]
        
        # 从其他车辆的碰撞风险中移除
        for risks in self.collision_risks.values():
            if vehicle_id in risks:
                del risks[vehicle_id]
    
    def detect_collisions(self, vehicle_id: str, search_radius: float = 100.0, 
                         time_window: float = 10.0) -> List[CollisionRisk]:
        """
        检测给定车辆的潜在碰撞
        
        Args:
            vehicle_id: 车辆ID
            search_radius: 搜索半径（米）
            time_window: 时间窗口（秒）
            
        Returns:
            碰撞风险列表
        """
        with Timer() as timer:
            # 获取车辆对象
            vehicle = self.vehicle_cache.get(vehicle_id)
            if not vehicle:
                return []
            
            # 阶段一：空间过滤
            nearby_vehicle_ids = self._spatial_filtering(vehicle_id, vehicle.position, search_radius)
            
            # 获取附近车辆对象
            nearby_vehicles = {}
            for other_id in nearby_vehicle_ids:
                other_vehicle = self.vehicle_cache.get(other_id)
                if other_vehicle:
                    nearby_vehicles[other_id] = other_vehicle
            
            # 阶段二：时间过滤
            potential_collisions = self._temporal_filtering(vehicle, nearby_vehicles, time_window)
            
            # 阶段三：精确碰撞检测
            collision_risks = []
            for other_id, time_to_closest, closest_distance in potential_collisions:
                other_vehicle = nearby_vehicles[other_id]
                
                collision_info = self._precise_collision_detection(
                    vehicle, other_vehicle, time_window
                )
                
                if collision_info:
                    # 阶段四：风险评估
                    risk_level = self._risk_assessment(vehicle, other_vehicle, collision_info)
                    
                    # 创建碰撞风险对象
                    risk = CollisionRisk(
                        id=f"risk-{uuid.uuid4()}",
                        vehicle_id=vehicle_id,
                        other_vehicle_id=other_id,
                        time_to_collision=collision_info["collision_time"],
                        distance=collision_info["distance"],
                        relative_speed=collision_info.get("relative_speed", 0.0),
                        risk_level=risk_level,
                        collision_position=collision_info["collision_position"],
                        timestamp=time.time()
                    )
                    
                    collision_risks.append(risk)
                    
                    # 更新碰撞风险缓存
                    if vehicle_id not in self.collision_risks:
                        self.collision_risks[vehicle_id] = {}
                    self.collision_risks[vehicle_id][other_id] = risk
            
            # 更新统计信息
            self.stats["total_detections"] += 1
            self.stats["potential_collisions"] += len(potential_collisions)
            self.stats["high_risk_collisions"] += len([r for r in collision_risks if r.risk_level > 0.7])
            
            detection_time_ms = timer.elapsed_ms
            self.stats["avg_detection_time_ms"] = (
                (self.stats["avg_detection_time_ms"] * (self.stats["total_detections"] - 1) + detection_time_ms) / 
                self.stats["total_detections"]
            )
            self.stats["max_detection_time_ms"] = max(self.stats["max_detection_time_ms"], detection_time_ms)
            
            if detection_time_ms > 50.0:
                logger.warning(f"Collision detection for vehicle {vehicle_id} took {detection_time_ms:.2f}ms, "
                              f"which exceeds the recommended 50ms threshold")
        
        return collision_risks
    
    def get_collision_risks(self, vehicle_id: str) -> List[CollisionRisk]:
        """
        获取车辆的碰撞风险
        
        Args:
            vehicle_id: 车辆ID
            
        Returns:
            碰撞风险列表
        """
        if vehicle_id not in self.collision_risks:
            return []
        
        return list(self.collision_risks[vehicle_id].values())
    
    def _spatial_filtering(self, vehicle_id: str, position: Position, search_radius: float) -> Set[str]:
        """
        空间过滤，获取给定位置附近的车辆
        
        Args:
            vehicle_id: 车辆ID
            position: 位置
            search_radius: 搜索半径（米）
            
        Returns:
            附近车辆ID集合
        """
        # 使用空间索引获取附近车辆
        nearby_vehicle_ids = self.spatial_index.get_nearby_vehicles(position, search_radius)
        
        # 排除自身
        if vehicle_id in nearby_vehicle_ids:
            nearby_vehicle_ids.remove(vehicle_id)
        
        return nearby_vehicle_ids
    
    def _temporal_filtering(self, vehicle: Vehicle, nearby_vehicles: Dict[str, Vehicle], 
                           time_window: float) -> List[Tuple[str, float, float]]:
        """
        时间过滤，过滤掉不可能在短时间内发生碰撞的车辆对
        
        Args:
            vehicle: 车辆对象
            nearby_vehicles: 附近车辆字典 {vehicle_id: Vehicle}
            time_window: 时间窗口（秒）
            
        Returns:
            潜在碰撞列表 [(vehicle_id, time_to_closest, closest_distance), ...]
        """
        potential_collisions = []
        
        for other_id, other_vehicle in nearby_vehicles.items():
            # 计算当前距离
            current_distance = self._calculate_distance(vehicle.position, other_vehicle.position)
            
            # 计算相对速度向量
            rel_velocity = Vector(
                x=vehicle.velocity.x - other_vehicle.velocity.x,
                y=vehicle.velocity.y - other_vehicle.velocity.y,
                z=vehicle.velocity.z - other_vehicle.velocity.z
            )
            
            # 计算相对位置向量
            rel_position = Vector(
                x=other_vehicle.position.x - vehicle.position.x,
                y=other_vehicle.position.y - vehicle.position.y,
                z=other_vehicle.position.z - vehicle.position.z
            )
            
            # 计算相对速度大小
            rel_speed = self._magnitude(rel_velocity)
            
            # 如果相对速度几乎为零，不太可能发生碰撞
            if rel_speed < 0.1:  # m/s
                continue
            
            # 计算相对位置和相对速度的点积
            dot_product = self._dot_product(rel_position, rel_velocity)
            
            # 如果点积为正，车辆正在远离
            if dot_product > 0 and current_distance > SAFE_DISTANCE_DEFAULT:
                continue
            
            # 计算最小接近时间
            time_to_closest = -dot_product / (rel_speed * rel_speed)
            
            # 如果最小接近时间为负或超出时间窗口，排除
            if time_to_closest < 0 or time_to_closest > time_window:
                continue
            
            # 计算最小距离
            closest_distance = self._distance_at_time(vehicle, other_vehicle, time_to_closest)
            
            # 如果最小距离大于安全距离，排除
            safe_distance = self._calculate_safe_distance(vehicle, other_vehicle)
            if closest_distance > safe_distance:
                continue
            
            # 添加到潜在碰撞列表
            potential_collisions.append((other_id, time_to_closest, closest_distance))
        
        return potential_collisions
    
    def _precise_collision_detection(self, vehicle: Vehicle, other_vehicle: Vehicle, 
                                    time_window: float, time_step: float = 0.1) -> Optional[Dict[str, Any]]:
        """
        精确碰撞检测，检查是否存在碰撞点
        
        Args:
            vehicle: 车辆对象
            other_vehicle: 其他车辆对象
            time_window: 时间窗口（秒）
            time_step: 时间步长（秒）
            
        Returns:
            碰撞信息字典或None
        """
        # 计算安全距离
        safe_distance = self._calculate_safe_distance(vehicle, other_vehicle)
        
        # 计算相对速度
        rel_velocity = Vector(
            x=vehicle.velocity.x - other_vehicle.velocity.x,
            y=vehicle.velocity.y - other_vehicle.velocity.y,
            z=vehicle.velocity.z - other_vehicle.velocity.z
        )
        rel_speed = self._magnitude(rel_velocity)
        
        # 在时间窗口内按时间步长检查
        for t in range(int(time_window / time_step)):
            current_time = t * time_step
            
            # 预测位置
            vehicle_pos = self._predict_position(vehicle, current_time)
            other_pos = self._predict_position(other_vehicle, current_time)
            
            # 计算距离
            distance = self._calculate_distance(vehicle_pos, other_pos)
            
            # 检查是否碰撞
            if distance <= safe_distance:
                return {
                    "collision_time": current_time,
                    "collision_position": self._midpoint(vehicle_pos, other_pos),
                    "distance": distance,
                    "safe_distance": safe_distance,
                    "relative_speed": rel_speed
                }
        
        return None
    
    def _risk_assessment(self, vehicle: Vehicle, other_vehicle: Vehicle, 
                        collision_info: Dict[str, Any]) -> float:
        """
        风险评估，计算碰撞风险等级
        
        Args:
            vehicle: 车辆对象
            other_vehicle: 其他车辆对象
            collision_info: 碰撞信息
            
        Returns:
            风险等级（0-1）
        """
        # 获取碰撞信息
        collision_time = collision_info["collision_time"]
        distance = collision_info["distance"]
        safe_distance = collision_info["safe_distance"]
        rel_speed = collision_info.get("relative_speed", 0.0)
        
        # 计算碰撞角度
        heading_diff = abs(vehicle.heading - other_vehicle.heading)
        angle_factor = math.sin(heading_diff)  # 垂直碰撞风险最高
        
        # 考虑车辆类型
        type_factor = self._get_type_factor(vehicle.type, other_vehicle.type)
        
        # 计算距离因子（越接近安全距离风险越高）
        distance_factor = 1.0 - (distance / safe_distance)
        
        # 计算时间因子（碰撞时间越短风险越高）
        time_factor = 1.0 - min(1.0, collision_time / MAX_WARNING_TIME)
        
        # 计算速度因子（相对速度越高风险越高）
        speed_factor = min(1.0, rel_speed / MAX_RELATIVE_SPEED)
        
        # 综合风险评估
        risk = (
            WEIGHT_DISTANCE * distance_factor +
            WEIGHT_TIME * time_factor +
            WEIGHT_SPEED * speed_factor +
            WEIGHT_ANGLE * angle_factor +
            WEIGHT_TYPE * type_factor
        )
        
        # 归一化到0-1范围
        return max(0.0, min(1.0, risk))
    
    def _calculate_distance(self, pos1: Position, pos2: Position) -> float:
        """
        计算两点之间的距离
        
        Args:
            pos1: 位置1
            pos2: 位置2
            
        Returns:
            距离（米）
        """
        return math.sqrt(
            (pos1.x - pos2.x) ** 2 +
            (pos1.y - pos2.y) ** 2 +
            (pos1.z - pos2.z) ** 2
        )
    
    def _magnitude(self, vector: Vector) -> float:
        """
        计算向量大小
        
        Args:
            vector: 向量
            
        Returns:
            向量大小
        """
        return math.sqrt(vector.x ** 2 + vector.y ** 2 + vector.z ** 2)
    
    def _dot_product(self, v1: Vector, v2: Vector) -> float:
        """
        计算向量点积
        
        Args:
            v1: 向量1
            v2: 向量2
            
        Returns:
            点积
        """
        return v1.x * v2.x + v1.y * v2.y + v1.z * v2.z
    
    def _predict_position(self, vehicle: Vehicle, time_delta: float) -> Position:
        """
        预测车辆位置
        
        Args:
            vehicle: 车辆对象
            time_delta: 时间增量（秒）
            
        Returns:
            预测位置
        """
        # 考虑加速度的预测
        return Position(
            x=vehicle.position.x + vehicle.velocity.x * time_delta + 0.5 * vehicle.acceleration.x * time_delta * time_delta,
            y=vehicle.position.y + vehicle.velocity.y * time_delta + 0.5 * vehicle.acceleration.y * time_delta * time_delta,
            z=vehicle.position.z + vehicle.velocity.z * time_delta + 0.5 * vehicle.acceleration.z * time_delta * time_delta
        )
    
    def _distance_at_time(self, vehicle1: Vehicle, vehicle2: Vehicle, time_delta: float) -> float:
        """
        计算给定时间点两车之间的距离
        
        Args:
            vehicle1: 车辆1
            vehicle2: 车辆2
            time_delta: 时间增量（秒）
            
        Returns:
            距离（米）
        """
        pos1 = self._predict_position(vehicle1, time_delta)
        pos2 = self._predict_position(vehicle2, time_delta)
        return self._calculate_distance(pos1, pos2)
    
    def _midpoint(self, pos1: Position, pos2: Position) -> Position:
        """
        计算两点的中点
        
        Args:
            pos1: 位置1
            pos2: 位置2
            
        Returns:
            中点位置
        """
        return Position(
            x=(pos1.x + pos2.x) / 2,
            y=(pos1.y + pos2.y) / 2,
            z=(pos1.z + pos2.z) / 2
        )
    
    def _calculate_safe_distance(self, vehicle1: Vehicle, vehicle2: Vehicle) -> float:
        """
        计算两车之间的安全距离
        
        Args:
            vehicle1: 车辆1
            vehicle2: 车辆2
            
        Returns:
            安全距离（米）
        """
        # 简单实现：车辆尺寸之和加上基础安全距离
        return (vehicle1.size + vehicle2.size) / 2 + SAFE_DISTANCE_DEFAULT
    
    def _get_type_factor(self, type1: str, type2: str) -> float:
        """
        获取车辆类型因子
        
        Args:
            type1: 车辆1类型
            type2: 车辆2类型
            
        Returns:
            类型因子（0-1）
        """
        # 简单实现：不同类型的车辆碰撞风险更高
        if type1 == type2:
            return 0.5
        else:
            return 0.8
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取碰撞检测器统计信息
        
        Returns:
            统计信息字典
        """
        return self.stats


class CollisionPredictionModel:
    """碰撞预测模型，用于预测未来碰撞风险"""
    
    def __init__(self, collision_detector: CollisionDetector):
        """
        初始化碰撞预测模型
        
        Args:
            collision_detector: 碰撞检测器
        """
        self.collision_detector = collision_detector
        
        # 历史轨迹缓存
        self.trajectory_history: Dict[str, List[Tuple[Position, float]]] = {}  # vehicle_id -> [(position, timestamp), ...]
        self.max_history_length = 100  # 最大历史记录长度
        
        # 预测模型参数
        self.prediction_horizon = 10.0  # 预测时间范围（秒）
        self.prediction_step = 0.5  # 预测时间步长（秒）
        
        # 统计信息
        self.stats = {
            "total_predictions": 0,
            "avg_prediction_time_ms": 0.0
        }
        
        logger.info("Collision prediction model initialized")
    
    def update_trajectory(self, vehicle_id: str, position: Position, timestamp: float) -> None:
        """
        更新车辆轨迹历史
        
        Args:
            vehicle_id: 车辆ID
            position: 位置
            timestamp: 时间戳
        """
        if vehicle_id not in self.trajectory_history:
            self.trajectory_history[vehicle_id] = []
        
        # 添加新位置
        self.trajectory_history[vehicle_id].append((position, timestamp))
        
        # 限制历史记录长度
        if len(self.trajectory_history[vehicle_id]) > self.max_history_length:
            self.trajectory_history[vehicle_id] = self.trajectory_history[vehicle_id][-self.max_history_length:]
    
    def predict_collisions(self, vehicle_id: str) -> List[CollisionRisk]:
        """
        预测车辆的未来碰撞风险
        
        Args:
            vehicle_id: 车辆ID
            
        Returns:
            预测的碰撞风险列表
        """
        with Timer() as timer:
            # 获取车辆对象
            vehicle = self.collision_detector.vehicle_cache.get(vehicle_id)
            if not vehicle:
                return []
            
            # 获取历史轨迹
            history = self.trajectory_history.get(vehicle_id, [])
            if len(history) < 2:
                # 历史数据不足，使用基本碰撞检测
                return self.collision_detector.detect_collisions(vehicle_id)
            
            # 分析历史轨迹模式
            pattern = self._analyze_trajectory_pattern(history)
            
            # 预测未来轨迹
            future_trajectory = self._predict_future_trajectory(vehicle, pattern)
            
            # 对每个预测点进行碰撞检测
            collision_risks = []
            for time_offset, predicted_position in future_trajectory:
                # 创建预测车辆对象
                predicted_vehicle = self._create_predicted_vehicle(vehicle, predicted_position, time_offset)
                
                # 在预测位置进行碰撞检测
                risks = self._detect_at_position(predicted_vehicle, time_offset)
                collision_risks.extend(risks)
            
            # 合并相同车辆对的碰撞风险，保留风险最高的
            merged_risks = self._merge_collision_risks(collision_risks)
            
            # 更新统计信息
            self.stats["total_predictions"] += 1
            prediction_time_ms = timer.elapsed_ms
            self.stats["avg_prediction_time_ms"] = (
                (self.stats["avg_prediction_time_ms"] * (self.stats["total_predictions"] - 1) + prediction_time_ms) / 
                self.stats["total_predictions"]
            )
        
        return merged_risks
    
    def _analyze_trajectory_pattern(self, history: List[Tuple[Position, float]]) -> Dict[str, Any]:
        """
        分析轨迹模式
        
        Args:
            history: 历史轨迹 [(position, timestamp), ...]
            
        Returns:
            轨迹模式
        """
        # 计算平均速度和加速度
        if len(history) < 2:
            return {"type": "unknown"}
        
        # 按时间排序
        sorted_history = sorted(history, key=lambda x: x[1])
        
        # 计算速度
        velocities = []
        for i in range(1, len(sorted_history)):
            pos1, t1 = sorted_history[i-1]
            pos2, t2 = sorted_history[i]
            
            dt = t2 - t1
            if dt > 0:
                vx = (pos2.x - pos1.x) / dt
                vy = (pos2.y - pos1.y) / dt
                vz = (pos2.z - pos1.z) / dt
                
                velocities.append((Vector(x=vx, y=vy, z=vz), t2))
        
        if not velocities:
            return {"type": "stationary"}
        
        # 计算加速度
        accelerations = []
        for i in range(1, len(velocities)):
            v1, t1 = velocities[i-1]
            v2, t2 = velocities[i]
            
            dt = t2 - t1
            if dt > 0:
                ax = (v2.x - v1.x) / dt
                ay = (v2.y - v1.y) / dt
                az = (v2.z - v1.z) / dt
                
                accelerations.append(Vector(x=ax, y=ay, z=az))
        
        # 计算平均速度和加速度
        avg_velocity = Vector(x=0, y=0, z=0)
        for v, _ in velocities:
            avg_velocity.x += v.x
            avg_velocity.y += v.y
            avg_velocity.z += v.z
        
        if velocities:
            avg_velocity.x /= len(velocities)
            avg_velocity.y /= len(velocities)
            avg_velocity.z /= len(velocities)
        
        avg_acceleration = Vector(x=0, y=0, z=0)
        for a in accelerations:
            avg_acceleration.x += a.x
            avg_acceleration.y += a.y
            avg_acceleration.z += a.z
        
        if accelerations:
            avg_acceleration.x /= len(accelerations)
            avg_acceleration.y /= len(accelerations)
            avg_acceleration.z /= len(accelerations)
        
        # 确定轨迹类型
        speed = math.sqrt(avg_velocity.x**2 + avg_velocity.y**2 + avg_velocity.z**2)
        accel = math.sqrt(avg_acceleration.x**2 + avg_acceleration.y**2 + avg_acceleration.z**2)
        
        if speed < 0.1:
            pattern_type = "stationary"
        elif accel < 0.1:
            pattern_type = "constant_velocity"
        else:
            pattern_type = "accelerating"
        
        return {
            "type": pattern_type,
            "avg_velocity": avg_velocity,
            "avg_acceleration": avg_acceleration,
            "last_position": sorted_history[-1][0],
            "last_timestamp": sorted_history[-1][1]
        }
    
    def _predict_future_trajectory(self, vehicle: Vehicle, pattern: Dict[str, Any]) -> List[Tuple[float, Position]]:
        """
        预测未来轨迹
        
        Args:
            vehicle: 车辆对象
            pattern: 轨迹模式
            
        Returns:
            预测轨迹 [(time_offset, position), ...]
        """
        trajectory = []
        pattern_type = pattern.get("type", "unknown")
        
        # 根据模式类型使用不同的预测方法
        if pattern_type == "stationary":
            # 静止车辆，位置不变
            for t in np.arange(0, self.prediction_horizon, self.prediction_step):
                trajectory.append((t, vehicle.position))
        
        elif pattern_type == "constant_velocity":
            # 匀速运动
            for t in np.arange(0, self.prediction_horizon, self.prediction_step):
                pos = Position(
                    x=vehicle.position.x + vehicle.velocity.x * t,
                    y=vehicle.position.y + vehicle.velocity.y * t,
                    z=vehicle.position.z + vehicle.velocity.z * t
                )
                trajectory.append((t, pos))
        
        elif pattern_type == "accelerating":
            # 加速运动
            for t in np.arange(0, self.prediction_horizon, self.prediction_step):
                pos = Position(
                    x=vehicle.position.x + vehicle.velocity.x * t + 0.5 * vehicle.acceleration.x * t * t,
                    y=vehicle.position.y + vehicle.velocity.y * t + 0.5 * vehicle.acceleration.y * t * t,
                    z=vehicle.position.z + vehicle.velocity.z * t + 0.5 * vehicle.acceleration.z * t * t
                )
                trajectory.append((t, pos))
        
        else:
            # 未知模式，使用当前速度和加速度
            for t in np.arange(0, self.prediction_horizon, self.prediction_step):
                pos = Position(
                    x=vehicle.position.x + vehicle.velocity.x * t + 0.5 * vehicle.acceleration.x * t * t,
                    y=vehicle.position.y + vehicle.velocity.y * t + 0.5 * vehicle.acceleration.y * t * t,
                    z=vehicle.position.z + vehicle.velocity.z * t + 0.5 * vehicle.acceleration.z * t * t
                )
                trajectory.append((t, pos))
        
        return trajectory
    
    def _create_predicted_vehicle(self, vehicle: Vehicle, position: Position, time_offset: float) -> Vehicle:
        """
        创建预测车辆对象
        
        Args:
            vehicle: 原车辆对象
            position: 预测位置
            time_offset: 时间偏移
            
        Returns:
            预测车辆对象
        """
        # 创建预测车辆对象，保持原车辆的属性，但更新位置
        return Vehicle(
            id=vehicle.id,
            position=position,
            velocity=vehicle.velocity,
            acceleration=vehicle.acceleration,
            heading=vehicle.heading,
            size=vehicle.size,
            type=vehicle.type,
            timestamp=vehicle.timestamp + time_offset
        )
    
    def _detect_at_position(self, predicted_vehicle: Vehicle, time_offset: float) -> List[CollisionRisk]:
        """
        在预测位置进行碰撞检测
        
        Args:
            predicted_vehicle: 预测车辆对象
            time_offset: 时间偏移
            
        Returns:
            碰撞风险列表
        """
        # 获取附近车辆
        nearby_vehicle_ids = self.collision_detector._spatial_filtering(
            predicted_vehicle.id, predicted_vehicle.position, 100.0
        )
        
        collision_risks = []
        
        # 对每个附近车辆进行碰撞检测
        for other_id in nearby_vehicle_ids:
            other_vehicle = self.collision_detector.vehicle_cache.get(other_id)
            if not other_vehicle:
                continue
            
            # 预测其他车辆的位置
            other_predicted_position = self.collision_detector._predict_position(other_vehicle, time_offset)
            other_predicted_vehicle = self._create_predicted_vehicle(
                other_vehicle, other_predicted_position, time_offset
            )
            
            # 检测碰撞
            collision_info = self.collision_detector._precise_collision_detection(
                predicted_vehicle, other_predicted_vehicle, 1.0
            )
            
            if collision_info:
                # 风险评估
                risk_level = self.collision_detector._risk_assessment(
                    predicted_vehicle, other_predicted_vehicle, collision_info
                )
                
                # 创建碰撞风险对象
                risk = CollisionRisk(
                    id=f"risk-{uuid.uuid4()}",
                    vehicle_id=predicted_vehicle.id,
                    other_vehicle_id=other_id,
                    time_to_collision=collision_info["collision_time"] + time_offset,
                    distance=collision_info["distance"],
                    relative_speed=collision_info.get("relative_speed", 0.0),
                    risk_level=risk_level,
                    collision_position=collision_info["collision_position"],
                    timestamp=time.time(),
                    is_predicted=True
                )
                
                collision_risks.append(risk)
        
        return collision_risks
    
    def _merge_collision_risks(self, risks: List[CollisionRisk]) -> List[CollisionRisk]:
        """
        合并相同车辆对的碰撞风险，保留风险最高的
        
        Args:
            risks: 碰撞风险列表
            
        Returns:
            合并后的碰撞风险列表
        """
        # 按车辆对分组
        risk_map = {}
        for risk in risks:
            key = tuple(sorted([risk.vehicle_id, risk.other_vehicle_id]))
            if key not in risk_map or risk.risk_level > risk_map[key].risk_level:
                risk_map[key] = risk
        
        return list(risk_map.values())
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取预测模型统计信息
        
        Returns:
            统计信息字典
        """
        return {
            **self.stats,
            "trajectory_history_count": len(self.trajectory_history)
        }
