"""
预警系统实现，用于及时发现潜在碰撞风险并通知相关车辆。
"""
import time
import uuid
import asyncio
import heapq
from typing import Dict, List, Set, Tuple, Optional, Any, Callable
from dataclasses import dataclass, field

from ..common.models import Position, Vehicle, CollisionRisk, Alert
from ..common.utils import get_logger, Timer
from ..messaging.messaging import MessageBroker, MessageConsumer, MessageProducer
from .collision_detection import CollisionDetector, CollisionPredictionModel

logger = get_logger(__name__)

# 风险等级阈值
RISK_LEVEL_LOW = 0.3
RISK_LEVEL_MEDIUM = 0.6
RISK_LEVEL_HIGH = 0.8

# 预警优先级
PRIORITY_LOW = 0
PRIORITY_MEDIUM = 1
PRIORITY_HIGH = 2
PRIORITY_CRITICAL = 3


@dataclass
class AlertInfo:
    """预警信息"""
    id: str  # 预警ID
    vehicle_id: str  # 车辆ID
    other_vehicle_id: str  # 其他车辆ID
    risk_level: float  # 风险等级（0-1）
    time_to_collision: float  # 碰撞时间（秒）
    message: str  # 预警消息
    priority: int  # 优先级
    timestamp: float  # 时间戳
    acknowledged: bool = False  # 是否已确认
    
    def __lt__(self, other):
        """用于优先级队列排序"""
        return (self.priority, -self.timestamp) > (other.priority, -other.timestamp)


class AlertManager:
    """预警管理器，负责生成和管理预警"""
    
    def __init__(self, broker: MessageBroker):
        """
        初始化预警管理器
        
        Args:
            broker: 消息代理
        """
        self.broker = broker
        
        # 预警缓存
        self.alerts: Dict[str, AlertInfo] = {}  # alert_id -> AlertInfo
        self.vehicle_alerts: Dict[str, Dict[str, str]] = {}  # vehicle_id -> {other_id -> alert_id}
        
        # 预警队列（优先级队列）
        self.alert_queue: List[AlertInfo] = []
        
        # 预警回调
        self.alert_callbacks: Dict[str, List[Callable[[Alert], None]]] = {}  # vehicle_id -> [callback, ...]
        
        # 消息通信
        self.alert_producer = MessageProducer(broker, default_topic="alerts")
        self.alert_consumer = MessageConsumer(
            broker=broker,
            topics=["alerts"],
            group_id=f"alert-manager-{uuid.uuid4()}"
        )
        
        # 任务
        self.running = False
        self.processing_task = None
        
        logger.info("Alert manager initialized")
    
    async def start(self) -> None:
        """启动预警管理器"""
        if self.running:
            return
        
        self.running = True
        
        # 启动消费者
        await self.alert_consumer.start()
        self.alert_consumer.on_message("alerts", self._handle_alert_message)
        
        # 启动处理任务
        self.processing_task = asyncio.create_task(self._processing_loop())
        
        logger.info("Alert manager started")
    
    async def stop(self) -> None:
        """停止预警管理器"""
        if not self.running:
            return
        
        self.running = False
        
        # 停止处理任务
        if self.processing_task:
            self.processing_task.cancel()
            try:
                await self.processing_task
            except asyncio.CancelledError:
                pass
        
        # 停止消费者
        await self.alert_consumer.stop()
        
        logger.info("Alert manager stopped")
    
    def create_alert(self, risk: CollisionRisk) -> AlertInfo:
        """
        根据碰撞风险创建预警
        
        Args:
            risk: 碰撞风险
            
        Returns:
            预警信息
        """
        # 确定风险等级和优先级
        priority = self._get_priority(risk.risk_level, risk.time_to_collision)
        
        # 生成预警消息
        message = self._generate_alert_message(risk)
        
        # 创建预警信息
        alert_id = f"alert-{uuid.uuid4()}"
        alert_info = AlertInfo(
            id=alert_id,
            vehicle_id=risk.vehicle_id,
            other_vehicle_id=risk.other_vehicle_id,
            risk_level=risk.risk_level,
            time_to_collision=risk.time_to_collision,
            message=message,
            priority=priority,
            timestamp=time.time()
        )
        
        # 存储预警信息
        self.alerts[alert_id] = alert_info
        
        # 更新车辆预警映射
        if risk.vehicle_id not in self.vehicle_alerts:
            self.vehicle_alerts[risk.vehicle_id] = {}
        self.vehicle_alerts[risk.vehicle_id][risk.other_vehicle_id] = alert_id
        
        # 添加到预警队列
        heapq.heappush(self.alert_queue, alert_info)
        
        return alert_info
    
    def update_alert(self, risk: CollisionRisk) -> Optional[AlertInfo]:
        """
        更新预警
        
        Args:
            risk: 碰撞风险
            
        Returns:
            更新后的预警信息，如果不存在则返回None
        """
        # 检查是否已存在预警
        if (risk.vehicle_id in self.vehicle_alerts and 
            risk.other_vehicle_id in self.vehicle_alerts[risk.vehicle_id]):
            alert_id = self.vehicle_alerts[risk.vehicle_id][risk.other_vehicle_id]
            alert_info = self.alerts.get(alert_id)
            
            if alert_info:
                # 更新预警信息
                old_priority = alert_info.priority
                
                alert_info.risk_level = risk.risk_level
                alert_info.time_to_collision = risk.time_to_collision
                alert_info.priority = self._get_priority(risk.risk_level, risk.time_to_collision)
                alert_info.message = self._generate_alert_message(risk)
                alert_info.timestamp = time.time()
                
                # 如果优先级变化，重新加入队列
                if alert_info.priority != old_priority:
                    # 重建队列（简单实现，实际应该使用更高效的方法）
                    self.alert_queue = [a for a in self.alert_queue if a.id != alert_id]
                    heapq.heapify(self.alert_queue)
                    heapq.heappush(self.alert_queue, alert_info)
                
                return alert_info
        
        return None
    
    def acknowledge_alert(self, alert_id: str) -> bool:
        """
        确认预警
        
        Args:
            alert_id: 预警ID
            
        Returns:
            是否成功确认
        """
        if alert_id in self.alerts:
            self.alerts[alert_id].acknowledged = True
            return True
        return False
    
    def get_alerts_for_vehicle(self, vehicle_id: str) -> List[AlertInfo]:
        """
        获取车辆的预警
        
        Args:
            vehicle_id: 车辆ID
            
        Returns:
            预警列表
        """
        if vehicle_id not in self.vehicle_alerts:
            return []
        
        alerts = []
        for alert_id in self.vehicle_alerts[vehicle_id].values():
            if alert_id in self.alerts:
                alerts.append(self.alerts[alert_id])
        
        # 按优先级排序
        return sorted(alerts, key=lambda a: (a.priority, -a.timestamp), reverse=True)
    
    def register_alert_callback(self, vehicle_id: str, callback: Callable[[Alert], None]) -> None:
        """
        注册预警回调
        
        Args:
            vehicle_id: 车辆ID
            callback: 回调函数 (Alert) -> None
        """
        if vehicle_id not in self.alert_callbacks:
            self.alert_callbacks[vehicle_id] = []
        
        self.alert_callbacks[vehicle_id].append(callback)
    
    def unregister_alert_callback(self, vehicle_id: str, callback: Callable[[Alert], None]) -> None:
        """
        注销预警回调
        
        Args:
            vehicle_id: 车辆ID
            callback: 回调函数
        """
        if vehicle_id in self.alert_callbacks:
            self.alert_callbacks[vehicle_id] = [cb for cb in self.alert_callbacks[vehicle_id] if cb != callback]
    
    def process_collision_risks(self, risks: List[CollisionRisk]) -> List[AlertInfo]:
        """
        处理碰撞风险，生成或更新预警
        
        Args:
            risks: 碰撞风险列表
            
        Returns:
            生成或更新的预警列表
        """
        alerts = []
        
        for risk in risks:
            # 检查风险等级是否达到预警阈值
            if risk.risk_level < RISK_LEVEL_LOW:
                continue
            
            # 检查是否已存在预警
            alert_info = self.update_alert(risk)
            
            # 如果不存在，创建新预警
            if not alert_info:
                alert_info = self.create_alert(risk)
            
            alerts.append(alert_info)
        
        return alerts
    
    def _get_priority(self, risk_level: float, time_to_collision: float) -> int:
        """
        根据风险等级和碰撞时间确定优先级
        
        Args:
            risk_level: 风险等级（0-1）
            time_to_collision: 碰撞时间（秒）
            
        Returns:
            优先级
        """
        # 紧急情况：高风险且碰撞时间短
        if risk_level >= RISK_LEVEL_HIGH and time_to_collision < 3.0:
            return PRIORITY_CRITICAL
        
        # 高优先级：高风险或碰撞时间短
        if risk_level >= RISK_LEVEL_HIGH or time_to_collision < 5.0:
            return PRIORITY_HIGH
        
        # 中优先级：中等风险
        if risk_level >= RISK_LEVEL_MEDIUM:
            return PRIORITY_MEDIUM
        
        # 低优先级：低风险
        return PRIORITY_LOW
    
    def _generate_alert_message(self, risk: CollisionRisk) -> str:
        """
        生成预警消息
        
        Args:
            risk: 碰撞风险
            
        Returns:
            预警消息
        """
        # 根据风险等级生成不同的消息
        if risk.risk_level >= RISK_LEVEL_HIGH:
            return f"紧急警告：与车辆 {risk.other_vehicle_id} 可能在 {risk.time_to_collision:.1f} 秒后发生碰撞，当前距离 {risk.distance:.1f} 米，请立即采取避让措施！"
        elif risk.risk_level >= RISK_LEVEL_MEDIUM:
            return f"警告：与车辆 {risk.other_vehicle_id} 可能在 {risk.time_to_collision:.1f} 秒后发生碰撞，当前距离 {risk.distance:.1f} 米，请注意避让。"
        else:
            return f"注意：与车辆 {risk.other_vehicle_id} 距离较近（{risk.distance:.1f} 米），请保持安全距离。"
    
    def _handle_alert_message(self, message) -> None:
        """
        处理预警消息
        
        Args:
            message: 消息对象
        """
        try:
            data = message.value
            msg_type = data["type"]
            
            if msg_type == "alert":
                self._handle_alert(data)
            elif msg_type == "acknowledge":
                self._handle_acknowledge(data)
        except Exception as e:
            logger.error(f"Error handling alert message: {e}")
    
    def _handle_alert(self, data) -> None:
        """
        处理预警消息
        
        Args:
            data: 消息数据
        """
        alert_id = data["alert_id"]
        vehicle_id = data["vehicle_id"]
        other_vehicle_id = data["other_vehicle_id"]
        risk_level = data["risk_level"]
        time_to_collision = data["time_to_collision"]
        message = data["message"]
        priority = data["priority"]
        timestamp = data["timestamp"]
        
        # 创建预警信息
        alert_info = AlertInfo(
            id=alert_id,
            vehicle_id=vehicle_id,
            other_vehicle_id=other_vehicle_id,
            risk_level=risk_level,
            time_to_collision=time_to_collision,
            message=message,
            priority=priority,
            timestamp=timestamp
        )
        
        # 存储预警信息
        self.alerts[alert_id] = alert_info
        
        # 更新车辆预警映射
        if vehicle_id not in self.vehicle_alerts:
            self.vehicle_alerts[vehicle_id] = {}
        self.vehicle_alerts[vehicle_id][other_vehicle_id] = alert_id
        
        # 添加到预警队列
        heapq.heappush(self.alert_queue, alert_info)
        
        # 调用回调函数
        self._notify_alert(alert_info)
    
    def _handle_acknowledge(self, data) -> None:
        """
        处理确认消息
        
        Args:
            data: 消息数据
        """
        alert_id = data["alert_id"]
        
        # 确认预警
        self.acknowledge_alert(alert_id)
    
    async def _processing_loop(self) -> None:
        """预警处理循环"""
        while self.running:
            try:
                # 处理预警队列
                await self._process_alert_queue()
                
                # 清理过期预警
                self._cleanup_expired_alerts()
                
                # 等待下一个处理周期
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in alert processing loop: {e}")
                await asyncio.sleep(1.0)
    
    async def _process_alert_queue(self) -> None:
        """处理预警队列"""
        # 处理队列中的预警
        while self.alert_queue and not self.alert_queue[0].acknowledged:
            # 获取最高优先级的预警
            alert_info = heapq.heappop(self.alert_queue)
            
            # 发送预警
            await self._send_alert(alert_info)
            
            # 如果预警未确认，重新加入队列（降低优先级）
            if not alert_info.acknowledged:
                # 避免频繁发送同一预警
                await asyncio.sleep(0.5)  # 短暂延迟
                heapq.heappush(self.alert_queue, alert_info)
    
    async def _send_alert(self, alert_info: AlertInfo) -> None:
        """
        发送预警
        
        Args:
            alert_info: 预警信息
        """
        # 创建预警消息
        message = {
            "type": "alert",
            "alert_id": alert_info.id,
            "vehicle_id": alert_info.vehicle_id,
            "other_vehicle_id": alert_info.other_vehicle_id,
            "risk_level": alert_info.risk_level,
            "time_to_collision": alert_info.time_to_collision,
            "message": alert_info.message,
            "priority": alert_info.priority,
            "timestamp": alert_info.timestamp
        }
        
        # 发送预警消息
        await self.alert_producer.send(message, key=alert_info.vehicle_id)
        
        # 调用回调函数
        self._notify_alert(alert_info)
    
    def _notify_alert(self, alert_info: AlertInfo) -> None:
        """
        通知预警
        
        Args:
            alert_info: 预警信息
        """
        # 创建预警对象
        alert = Alert(
            id=alert_info.id,
            vehicle_id=alert_info.vehicle_id,
            other_vehicle_id=alert_info.other_vehicle_id,
            risk_level=alert_info.risk_level,
            time_to_collision=alert_info.time_to_collision,
            message=alert_info.message,
            priority=alert_info.priority,
            timestamp=alert_info.timestamp
        )
        
        # 调用回调函数
        if alert_info.vehicle_id in self.alert_callbacks:
            for callback in self.alert_callbacks[alert_info.vehicle_id]:
                try:
                    callback(alert)
                except Exception as e:
                    logger.error(f"Error in alert callback: {e}")
    
    def _cleanup_expired_alerts(self) -> None:
        """清理过期预警"""
        now = time.time()
        expired_ids = []
        
        # 找出过期预警
        for alert_id, alert_info in self.alerts.items():
            # 预警已确认或已过期（超过30秒）
            if alert_info.acknowledged or now - alert_info.timestamp > 30.0:
                expired_ids.append(alert_id)
        
        # 移除过期预警
        for alert_id in expired_ids:
            if alert_id in self.alerts:
                alert_info = self.alerts[alert_id]
                
                # 从车辆预警映射中移除
                if alert_info.vehicle_id in self.vehicle_alerts:
                    if alert_info.other_vehicle_id in self.vehicle_alerts[alert_info.vehicle_id]:
                        del self.vehicle_alerts[alert_info.vehicle_id][alert_info.other_vehicle_id]
                
                # 从预警缓存中移除
                del self.alerts[alert_id]
        
        # 重建预警队列（移除过期预警）
        if expired_ids:
            self.alert_queue = [a for a in self.alert_queue if a.id not in expired_ids]
            heapq.heapify(self.alert_queue)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取预警管理器统计信息
        
        Returns:
            统计信息字典
        """
        # 计算各优先级预警数量
        priority_counts = {
            "critical": 0,
            "high": 0,
            "medium": 0,
            "low": 0
        }
        
        for alert in self.alerts.values():
            if alert.priority == PRIORITY_CRITICAL:
                priority_counts["critical"] += 1
            elif alert.priority == PRIORITY_HIGH:
                priority_counts["high"] += 1
            elif alert.priority == PRIORITY_MEDIUM:
                priority_counts["medium"] += 1
            else:
                priority_counts["low"] += 1
        
        return {
            "total_alerts": len(self.alerts),
            "active_alerts": len(self.alert_queue),
            "vehicle_count": len(self.vehicle_alerts),
            "priority_counts": priority_counts
        }


class EarlyWarningSystem:
    """早期预警系统，集成碰撞检测和预警管理"""
    
    def __init__(self, broker: MessageBroker, 
                collision_detector: CollisionDetector,
                prediction_model: CollisionPredictionModel):
        """
        初始化早期预警系统
        
        Args:
            broker: 消息代理
            collision_detector: 碰撞检测器
            prediction_model: 碰撞预测模型
        """
        self.broker = broker
        self.collision_detector = collision_detector
        self.prediction_model = prediction_model
        
        # 预警管理器
        self.alert_manager = AlertManager(broker)
        
        # 消息通信
        self.vehicle_consumer = MessageConsumer(
            broker=broker,
            topics=["vehicle-positions"],
            group_id=f"warning-system-{uuid.uuid4()}"
        )
        
        # 任务
        self.running = False
        self.detection_task = None
        
        logger.info("Early warning system initialized")
    
    async def start(self) -> None:
        """启动早期预警系统"""
        if self.running:
            return
        
        self.running = True
        
        # 启动预警管理器
        await self.alert_manager.start()
        
        # 启动消费者
        await self.vehicle_consumer.start()
        self.vehicle_consumer.on_message("vehicle-positions", self._handle_vehicle_position)
        
        # 启动检测任务
        self.detection_task = asyncio.create_task(self._detection_loop())
        
        logger.info("Early warning system started")
    
    async def stop(self) -> None:
        """停止早期预警系统"""
        if not self.running:
            return
        
        self.running = False
        
        # 停止检测任务
        if self.detection_task:
            self.detection_task.cancel()
            try:
                await self.detection_task
            except asyncio.CancelledError:
                pass
        
        # 停止消费者
        await self.vehicle_consumer.stop()
        
        # 停止预警管理器
        await self.alert_manager.stop()
        
        logger.info("Early warning system stopped")
    
    def register_alert_callback(self, vehicle_id: str, callback: Callable[[Alert], None]) -> None:
        """
        注册预警回调
        
        Args:
            vehicle_id: 车辆ID
            callback: 回调函数 (Alert) -> None
        """
        self.alert_manager.register_alert_callback(vehicle_id, callback)
    
    def _handle_vehicle_position(self, message) -> None:
        """
        处理车辆位置消息
        
        Args:
            message: 消息对象
        """
        try:
            data = message.value
            
            # 创建车辆对象
            vehicle = Vehicle(
                id=data["id"],
                position=Position(
                    x=data["position"]["x"],
                    y=data["position"]["y"],
                    z=data["position"]["z"]
                ),
                velocity=Vector(
                    x=data["velocity"]["x"],
                    y=data["velocity"]["y"],
                    z=data["velocity"]["z"]
                ),
                acceleration=Vector(
                    x=data["acceleration"]["x"],
                    y=data["acceleration"]["y"],
                    z=data["acceleration"]["z"]
                ),
                heading=data["heading"],
                size=data["size"],
                type=data["type"],
                timestamp=data["timestamp"]
            )
            
            # 更新车辆状态
            self.collision_detector.update_vehicle(vehicle)
            
            # 更新轨迹历史
            self.prediction_model.update_trajectory(vehicle.id, vehicle.position, vehicle.timestamp)
        except Exception as e:
            logger.error(f"Error handling vehicle position: {e}")
    
    async def _detection_loop(self) -> None:
        """碰撞检测循环"""
        while self.running:
            try:
                # 对所有车辆进行碰撞检测
                await self._detect_all_vehicles()
                
                # 等待下一个检测周期
                await asyncio.sleep(0.5)  # 2Hz检测频率
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in collision detection loop: {e}")
                await asyncio.sleep(1.0)
    
    async def _detect_all_vehicles(self) -> None:
        """对所有车辆进行碰撞检测"""
        with Timer() as timer:
            # 获取所有车辆
            vehicles = list(self.collision_detector.vehicle_cache.keys())
            
            # 对每个车辆进行碰撞检测
            for vehicle_id in vehicles:
                # 使用预测模型进行碰撞预测
                risks = self.prediction_model.predict_collisions(vehicle_id)
                
                # 处理碰撞风险，生成预警
                if risks:
                    self.alert_manager.process_collision_risks(risks)
            
            # 记录检测时间
            detection_time_ms = timer.elapsed_ms
            if detection_time_ms > 100.0:
                logger.warning(f"Collision detection for all vehicles took {detection_time_ms:.2f}ms, "
                              f"which exceeds the recommended 100ms threshold")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取早期预警系统统计信息
        
        Returns:
            统计信息字典
        """
        return {
            "collision_detector": self.collision_detector.get_stats(),
            "prediction_model": self.prediction_model.get_stats(),
            "alert_manager": self.alert_manager.get_stats()
        }
