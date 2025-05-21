"""
集成模块，将碰撞检测系统集成到分布式计算平台中。
"""
import os
import time
import uuid
import asyncio
import argparse
from typing import Dict, List, Set, Tuple, Optional, Any

from common.models import Position, Vector, Vehicle, CollisionRisk, Alert, NodeInfo, LoadMetrics
from common.utils import get_logger, Timer, setup_logging
from messaging.messaging import MessageBroker, MessageConsumer, MessageProducer
from compute.compute_node import ComputeNode
from scheduler.scheduler import TaskScheduler
from storage.storage import StorageManager
from reliability.high_availability import HeartbeatMonitor, LeaderElection
from reliability.disaster_recovery import BackupManager, StateTransfer
from reliability.failover_throttling import FailoverManager, ThrottlingManager
from collision.spatial_index import SpatialIndex, SpatialPartitioner
from collision.collision_detection import CollisionDetector, CollisionPredictionModel
from collision.data_sharding import ShardManager, LoadBalancer
from collision.warning_system import AlertManager, EarlyWarningSystem
from api.api import ApiServer

logger = get_logger(__name__)


class CollisionDetectionSystem:
    """碰撞检测系统，集成所有组件"""
    
    def __init__(self, node_id: str, broker_url: str, storage_url: str, api_port: int = 8000):
        """
        初始化碰撞检测系统
        
        Args:
            node_id: 节点ID
            broker_url: 消息代理URL
            storage_url: 存储URL
            api_port: API端口
        """
        self.node_id = node_id
        self.broker_url = broker_url
        self.storage_url = storage_url
        self.api_port = api_port
        
        # 组件实例
        self.broker = None
        self.storage_manager = None
        self.compute_node = None
        self.task_scheduler = None
        self.heartbeat_monitor = None
        self.leader_election = None
        self.backup_manager = None
        self.state_transfer = None
        self.failover_manager = None
        self.throttling_manager = None
        self.spatial_index = None
        self.spatial_partitioner = None
        self.collision_detector = None
        self.prediction_model = None
        self.shard_manager = None
        self.load_balancer = None
        self.alert_manager = None
        self.warning_system = None
        self.api_server = None
        
        # 任务
        self.running = False
        self.main_task = None
        
        logger.info(f"Collision detection system initialized for node {node_id}")
    
    async def start(self) -> None:
        """启动碰撞检测系统"""
        if self.running:
            return
        
        self.running = True
        
        # 初始化组件
        await self._initialize_components()
        
        # 启动组件
        await self._start_components()
        
        # 启动主任务
        self.main_task = asyncio.create_task(self._main_loop())
        
        logger.info(f"Collision detection system started for node {self.node_id}")
    
    async def stop(self) -> None:
        """停止碰撞检测系统"""
        if not self.running:
            return
        
        self.running = False
        
        # 停止主任务
        if self.main_task:
            self.main_task.cancel()
            try:
                await self.main_task
            except asyncio.CancelledError:
                pass
        
        # 停止组件
        await self._stop_components()
        
        logger.info(f"Collision detection system stopped for node {self.node_id}")
    
    async def _initialize_components(self) -> None:
        """初始化组件"""
        # 初始化消息代理
        self.broker = MessageBroker(self.broker_url)
        await self.broker.connect()
        
        # 初始化存储管理器
        self.storage_manager = StorageManager(self.storage_url)
        await self.storage_manager.connect()
        
        # 初始化计算节点
        self.compute_node = ComputeNode(
            node_id=self.node_id,
            broker=self.broker,
            storage=self.storage_manager
        )
        
        # 初始化任务调度器
        self.task_scheduler = TaskScheduler(
            broker=self.broker,
            node_id=self.node_id
        )
        
        # 初始化高可用性组件
        self.heartbeat_monitor = HeartbeatMonitor(
            broker=self.broker,
            node_id=self.node_id
        )
        
        self.leader_election = LeaderElection(
            broker=self.broker,
            node_id=self.node_id
        )
        
        # 初始化灾难恢复组件
        self.backup_manager = BackupManager(
            broker=self.broker,
            node_id=self.node_id,
            storage=self.storage_manager
        )
        
        self.state_transfer = StateTransfer(
            broker=self.broker,
            node_id=self.node_id,
            storage=self.storage_manager
        )
        
        # 初始化故障转移和限流组件
        self.failover_manager = FailoverManager(
            broker=self.broker,
            node_id=self.node_id
        )
        
        self.throttling_manager = ThrottlingManager(
            broker=self.broker,
            node_id=self.node_id
        )
        
        # 初始化碰撞检测组件
        self.spatial_index = SpatialIndex(
            base_size=(1000.0, 1000.0, 100.0),
            min_size=(10.0, 10.0, 5.0),
            max_level=3
        )
        
        self.spatial_partitioner = SpatialPartitioner(
            spatial_index=self.spatial_index,
            num_shards=10
        )
        
        self.collision_detector = CollisionDetector(
            spatial_index=self.spatial_index
        )
        
        self.prediction_model = CollisionPredictionModel(
            collision_detector=self.collision_detector
        )
        
        # 初始化数据分片和负载均衡组件
        self.shard_manager = ShardManager(
            broker=self.broker,
            node_id=self.node_id,
            spatial_partitioner=self.spatial_partitioner
        )
        
        self.load_balancer = LoadBalancer(
            broker=self.broker,
            node_id=self.node_id,
            shard_manager=self.shard_manager
        )
        
        # 初始化预警系统组件
        self.alert_manager = AlertManager(
            broker=self.broker
        )
        
        self.warning_system = EarlyWarningSystem(
            broker=self.broker,
            collision_detector=self.collision_detector,
            prediction_model=self.prediction_model
        )
        
        # 初始化API服务器
        self.api_server = ApiServer(
            port=self.api_port,
            broker=self.broker,
            storage=self.storage_manager,
            collision_system=self
        )
        
        logger.info(f"Components initialized for node {self.node_id}")
    
    async def _start_components(self) -> None:
        """启动组件"""
        # 启动计算节点
        await self.compute_node.start()
        
        # 启动任务调度器
        await self.task_scheduler.start()
        
        # 启动高可用性组件
        await self.heartbeat_monitor.start()
        await self.leader_election.start()
        
        # 启动灾难恢复组件
        await self.backup_manager.start()
        await self.state_transfer.start()
        
        # 启动故障转移和限流组件
        await self.failover_manager.start()
        await self.throttling_manager.start()
        
        # 启动数据分片和负载均衡组件
        await self.shard_manager.start()
        await self.load_balancer.start()
        
        # 启动预警系统组件
        await self.warning_system.start()
        
        # 启动API服务器
        await self.api_server.start()
        
        # 注册回调函数
        self._register_callbacks()
        
        logger.info(f"Components started for node {self.node_id}")
    
    async def _stop_components(self) -> None:
        """停止组件"""
        # 停止API服务器
        await self.api_server.stop()
        
        # 停止预警系统组件
        await self.warning_system.stop()
        
        # 停止数据分片和负载均衡组件
        await self.load_balancer.stop()
        await self.shard_manager.stop()
        
        # 停止故障转移和限流组件
        await self.throttling_manager.stop()
        await self.failover_manager.stop()
        
        # 停止灾难恢复组件
        await self.state_transfer.stop()
        await self.backup_manager.stop()
        
        # 停止高可用性组件
        await self.leader_election.stop()
        await self.heartbeat_monitor.stop()
        
        # 停止任务调度器
        await self.task_scheduler.stop()
        
        # 停止计算节点
        await self.compute_node.stop()
        
        # 关闭存储连接
        await self.storage_manager.disconnect()
        
        # 关闭消息代理连接
        await self.broker.disconnect()
        
        logger.info(f"Components stopped for node {self.node_id}")
    
    def _register_callbacks(self) -> None:
        """注册回调函数"""
        # 注册故障检测回调
        self.heartbeat_monitor.on_node_failure(self._handle_node_failure)
        
        # 注册领导者选举回调
        self.leader_election.on_leader_elected(self._handle_leader_elected)
        
        # 注册分片分配回调
        self.shard_manager.on_shard_assignment(self._handle_shard_assignment)
        self.shard_manager.on_shard_release(self._handle_shard_release)
        
        # 注册预警回调
        self.warning_system.register_alert_callback(self.node_id, self._handle_alert)
    
    def _handle_node_failure(self, node_id: str) -> None:
        """
        处理节点故障
        
        Args:
            node_id: 故障节点ID
        """
        logger.warning(f"Node failure detected: {node_id}")
        
        # 触发故障转移
        asyncio.create_task(self.failover_manager.handle_node_failure(node_id))
    
    def _handle_leader_elected(self, leader_id: str) -> None:
        """
        处理领导者选举
        
        Args:
            leader_id: 领导者节点ID
        """
        logger.info(f"Leader elected: {leader_id}")
        
        # 如果当前节点是领导者，执行领导者任务
        if leader_id == self.node_id:
            asyncio.create_task(self._run_leader_tasks())
    
    def _handle_shard_assignment(self, shard_id: str, node_id: str) -> None:
        """
        处理分片分配
        
        Args:
            shard_id: 分片ID
            node_id: 节点ID
        """
        logger.info(f"Shard {shard_id} assigned to node {node_id}")
        
        # 如果分配给当前节点，启动分片处理
        if node_id == self.node_id:
            asyncio.create_task(self._start_shard_processing(shard_id))
    
    def _handle_shard_release(self, shard_id: str, node_id: str) -> None:
        """
        处理分片释放
        
        Args:
            shard_id: 分片ID
            node_id: 节点ID
        """
        logger.info(f"Shard {shard_id} released from node {node_id}")
        
        # 如果从当前节点释放，停止分片处理
        if node_id == self.node_id:
            asyncio.create_task(self._stop_shard_processing(shard_id))
    
    def _handle_alert(self, alert: Alert) -> None:
        """
        处理预警
        
        Args:
            alert: 预警对象
        """
        logger.info(f"Alert received: {alert.message}")
        
        # 存储预警
        asyncio.create_task(self._store_alert(alert))
    
    async def _run_leader_tasks(self) -> None:
        """运行领导者任务"""
        try:
            # 执行负载均衡
            await self.load_balancer._check_balance()
            
            # 执行备份
            await self.backup_manager.create_backup()
        except Exception as e:
            logger.error(f"Error running leader tasks: {e}")
    
    async def _start_shard_processing(self, shard_id: str) -> None:
        """
        启动分片处理
        
        Args:
            shard_id: 分片ID
        """
        try:
            # 注册分片处理任务
            self.task_scheduler.register_task(
                task_id=f"process-shard-{shard_id}",
                task_type="shard-processing",
                params={"shard_id": shard_id},
                callback=self._process_shard
            )
            
            logger.info(f"Started processing for shard {shard_id}")
        except Exception as e:
            logger.error(f"Error starting shard processing: {e}")
    
    async def _stop_shard_processing(self, shard_id: str) -> None:
        """
        停止分片处理
        
        Args:
            shard_id: 分片ID
        """
        try:
            # 注销分片处理任务
            self.task_scheduler.unregister_task(f"process-shard-{shard_id}")
            
            logger.info(f"Stopped processing for shard {shard_id}")
        except Exception as e:
            logger.error(f"Error stopping shard processing: {e}")
    
    async def _process_shard(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理分片
        
        Args:
            params: 参数字典
            
        Returns:
            处理结果
        """
        shard_id = params["shard_id"]
        
        try:
            # 获取分片中的车辆
            vehicles = await self._get_vehicles_in_shard(shard_id)
            
            # 对每个车辆进行碰撞检测
            for vehicle_id in vehicles:
                # 使用预测模型进行碰撞预测
                risks = self.prediction_model.predict_collisions(vehicle_id)
                
                # 处理碰撞风险，生成预警
                if risks:
                    self.warning_system.alert_manager.process_collision_risks(risks)
            
            return {
                "shard_id": shard_id,
                "vehicle_count": len(vehicles),
                "processed_at": time.time()
            }
        except Exception as e:
            logger.error(f"Error processing shard {shard_id}: {e}")
            return {
                "shard_id": shard_id,
                "error": str(e),
                "processed_at": time.time()
            }
    
    async def _get_vehicles_in_shard(self, shard_id: str) -> List[str]:
        """
        获取分片中的车辆
        
        Args:
            shard_id: 分片ID
            
        Returns:
            车辆ID列表
        """
        # 从存储中获取分片车辆信息
        try:
            result = await self.storage_manager.get(f"shard:{shard_id}:vehicles")
            if result:
                return result.get("vehicle_ids", [])
        except Exception as e:
            logger.error(f"Error getting vehicles in shard {shard_id}: {e}")
        
        return []
    
    async def _store_alert(self, alert: Alert) -> None:
        """
        存储预警
        
        Args:
            alert: 预警对象
        """
        try:
            # 存储预警信息
            await self.storage_manager.set(
                key=f"alert:{alert.id}",
                value=alert.__dict__,
                expire=3600  # 1小时过期
            )
            
            # 更新车辆预警列表
            await self.storage_manager.append(
                key=f"vehicle:{alert.vehicle_id}:alerts",
                value=alert.id
            )
            
            logger.debug(f"Stored alert {alert.id} for vehicle {alert.vehicle_id}")
        except Exception as e:
            logger.error(f"Error storing alert: {e}")
    
    async def _main_loop(self) -> None:
        """主循环"""
        while self.running:
            try:
                # 更新节点负载
                load_metrics = await self._collect_load_metrics()
                self.load_balancer.update_load(self.node_id, load_metrics)
                
                # 等待下一个循环
                await asyncio.sleep(5.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                await asyncio.sleep(5.0)
    
    async def _collect_load_metrics(self) -> LoadMetrics:
        """
        收集负载指标
        
        Returns:
            负载指标
        """
        # 收集CPU、内存、网络和磁盘使用率
        cpu_usage = await self._get_cpu_usage()
        memory_usage = await self._get_memory_usage()
        network_usage = await self._get_network_usage()
        disk_usage = await self._get_disk_usage()
        
        # 收集任务队列大小和处理速率
        task_queue_size = self.task_scheduler.get_queue_size()
        processing_rate = self.task_scheduler.get_processing_rate()
        
        # 收集平均延迟
        average_latency = self.task_scheduler.get_average_latency()
        
        return LoadMetrics(
            cpu_usage=cpu_usage,
            memory_usage=memory_usage,
            network_usage=network_usage,
            disk_usage=disk_usage,
            task_queue_size=task_queue_size,
            processing_rate=processing_rate,
            average_latency=average_latency
        )
    
    async def _get_cpu_usage(self) -> float:
        """
        获取CPU使用率
        
        Returns:
            CPU使用率（0-1）
        """
        try:
            # 使用psutil获取CPU使用率
            import psutil
            return psutil.cpu_percent(interval=0.1) / 100.0
        except ImportError:
            # 如果没有psutil，返回随机值
            import random
            return random.uniform(0.1, 0.5)
    
    async def _get_memory_usage(self) -> float:
        """
        获取内存使用率
        
        Returns:
            内存使用率（0-1）
        """
        try:
            # 使用psutil获取内存使用率
            import psutil
            return psutil.virtual_memory().percent / 100.0
        except ImportError:
            # 如果没有psutil，返回随机值
            import random
            return random.uniform(0.2, 0.6)
    
    async def _get_network_usage(self) -> float:
        """
        获取网络使用率
        
        Returns:
            网络使用率（0-1）
        """
        # 简单实现，返回随机值
        import random
        return random.uniform(0.1, 0.4)
    
    async def _get_disk_usage(self) -> float:
        """
        获取磁盘使用率
        
        Returns:
            磁盘使用率（0-1）
        """
        try:
            # 使用psutil获取磁盘使用率
            import psutil
            return psutil.disk_usage('/').percent / 100.0
        except ImportError:
            # 如果没有psutil，返回随机值
            import random
            return random.uniform(0.3, 0.7)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取系统统计信息
        
        Returns:
            统计信息字典
        """
        return {
            "node_id": self.node_id,
            "is_leader": self.leader_election.is_leader(),
            "compute_node": self.compute_node.get_stats(),
            "task_scheduler": self.task_scheduler.get_stats(),
            "spatial_index": self.spatial_index.get_stats(),
            "collision_detector": self.collision_detector.get_stats(),
            "prediction_model": self.prediction_model.get_stats(),
            "shard_manager": self.shard_manager.get_stats(),
            "load_balancer": self.load_balancer.get_stats(),
            "warning_system": self.warning_system.get_stats()
        }


async def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="Collision Detection System")
    parser.add_argument("--node-id", type=str, default=f"node-{uuid.uuid4()}", help="Node ID")
    parser.add_argument("--broker-url", type=str, default="localhost:9092", help="Message broker URL")
    parser.add_argument("--storage-url", type=str, default="redis://localhost:6379", help="Storage URL")
    parser.add_argument("--api-port", type=int, default=8000, help="API port")
    parser.add_argument("--log-level", type=str, default="INFO", help="Log level")
    args = parser.parse_args()
    
    # 设置日志
    setup_logging(level=args.log_level)
    
    # 创建并启动系统
    system = CollisionDetectionSystem(
        node_id=args.node_id,
        broker_url=args.broker_url,
        storage_url=args.storage_url,
        api_port=args.api_port
    )
    
    try:
        await system.start()
        
        # 等待终止信号
        while True:
            await asyncio.sleep(1.0)
    except KeyboardInterrupt:
        logger.info("Received termination signal")
    finally:
        await system.stop()


if __name__ == "__main__":
    asyncio.run(main())
