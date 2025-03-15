"""
数据分片与负载均衡实现，用于处理数据倾斜和实现高吞吐量。
"""
import time
import uuid
import random
import asyncio
from typing import Dict, List, Set, Tuple, Optional, Any, Callable
from dataclasses import dataclass, field

from ..common.models import Position, Vehicle, CollisionRisk, NodeInfo, LoadMetrics
from ..common.utils import get_logger, Timer
from ..messaging.messaging import MessageBroker, MessageConsumer, MessageProducer
from ..compute.compute_node import ComputeNode
from ..scheduler.scheduler import TaskScheduler
from .spatial_index import SpatialIndex, SpatialPartitioner
from .collision_detection import CollisionDetector, CollisionPredictionModel

logger = get_logger(__name__)


class ShardManager:
    """分片管理器，负责数据分片和负载均衡"""
    
    def __init__(self, broker: MessageBroker, node_id: str, 
                spatial_partitioner: SpatialPartitioner,
                initial_shards: int = 10,
                rebalance_interval: float = 60.0):
        """
        初始化分片管理器
        
        Args:
            broker: 消息代理
            node_id: 节点ID
            spatial_partitioner: 空间分区器
            initial_shards: 初始分片数量
            rebalance_interval: 重新平衡间隔（秒）
        """
        self.broker = broker
        self.node_id = node_id
        self.spatial_partitioner = spatial_partitioner
        self.rebalance_interval = rebalance_interval
        
        # 分片数据结构
        self.shards: Dict[str, Dict[str, Any]] = {}  # shard_id -> shard_info
        self.node_shards: Dict[str, Set[str]] = {}  # node_id -> {shard_id, ...}
        self.vehicle_to_shard: Dict[str, str] = {}  # vehicle_id -> shard_id
        
        # 节点负载信息
        self.node_loads: Dict[str, LoadMetrics] = {}  # node_id -> LoadMetrics
        
        # 消息通信
        self.shard_producer = MessageProducer(broker, default_topic="shard-management")
        self.shard_consumer = MessageConsumer(
            broker=broker,
            topics=["shard-management"],
            group_id=f"shard-manager-{node_id}"
        )
        
        # 回调函数
        self.shard_assignment_callbacks: List[Callable[[str, str], None]] = []  # [(shard_id, node_id) -> None, ...]
        self.shard_release_callbacks: List[Callable[[str, str], None]] = []  # [(shard_id, node_id) -> None, ...]
        
        # 任务
        self.running = False
        self.management_task = None
        
        # 初始化分片
        self._initialize_shards(initial_shards)
        
        logger.info(f"Shard manager initialized with {initial_shards} shards")
    
    def _initialize_shards(self, initial_shards: int) -> None:
        """
        初始化分片
        
        Args:
            initial_shards: 初始分片数量
        """
        for i in range(initial_shards):
            shard_id = f"shard-{i}"
            self.shards[shard_id] = {
                "id": shard_id,
                "node_id": None,
                "vehicle_count": 0,
                "load": 0.0,
                "created_at": time.time()
            }
    
    async def start(self) -> None:
        """启动分片管理器"""
        if self.running:
            return
        
        self.running = True
        
        # 启动消费者
        await self.shard_consumer.start()
        self.shard_consumer.on_message("shard-management", self._handle_shard_message)
        
        # 启动管理任务
        self.management_task = asyncio.create_task(self._management_loop())
        
        logger.info(f"Shard manager started for node {self.node_id}")
    
    async def stop(self) -> None:
        """停止分片管理器"""
        if not self.running:
            return
        
        self.running = False
        
        # 停止管理任务
        if self.management_task:
            self.management_task.cancel()
            try:
                await self.management_task
            except asyncio.CancelledError:
                pass
        
        # 停止消费者
        await self.shard_consumer.stop()
        
        logger.info(f"Shard manager stopped for node {self.node_id}")
    
    def register_node(self, node_id: str) -> None:
        """
        注册节点
        
        Args:
            node_id: 节点ID
        """
        if node_id not in self.node_shards:
            self.node_shards[node_id] = set()
            logger.info(f"Registered node {node_id}")
    
    def unregister_node(self, node_id: str) -> None:
        """
        注销节点
        
        Args:
            node_id: 节点ID
        """
        if node_id in self.node_shards:
            # 重新分配该节点的分片
            shards_to_reassign = list(self.node_shards[node_id])
            for shard_id in shards_to_reassign:
                self._reassign_shard(shard_id, node_id)
            
            del self.node_shards[node_id]
            logger.info(f"Unregistered node {node_id}")
    
    def update_node_load(self, node_id: str, load_metrics: LoadMetrics) -> None:
        """
        更新节点负载
        
        Args:
            node_id: 节点ID
            load_metrics: 负载指标
        """
        self.node_loads[node_id] = load_metrics
        
        # 更新分片负载
        if node_id in self.node_shards:
            for shard_id in self.node_shards[node_id]:
                if shard_id in self.shards:
                    # 简单地将节点负载按分片数量平均分配
                    shard_count = len(self.node_shards[node_id])
                    if shard_count > 0:
                        self.shards[shard_id]["load"] = load_metrics.cpu_usage / shard_count
    
    def get_shard_for_vehicle(self, vehicle_id: str, position: Position) -> str:
        """
        获取车辆所属的分片
        
        Args:
            vehicle_id: 车辆ID
            position: 车辆位置
            
        Returns:
            分片ID
        """
        # 如果车辆已经分配了分片，返回该分片
        if vehicle_id in self.vehicle_to_shard:
            return self.vehicle_to_shard[vehicle_id]
        
        # 否则，使用空间分区器确定分片
        shard_id = self.spatial_partitioner.get_shard_for_position(position)
        
        # 如果找不到分片，随机分配一个
        if not shard_id or shard_id not in self.shards:
            shard_id = random.choice(list(self.shards.keys()))
        
        # 记录车辆到分片的映射
        self.vehicle_to_shard[vehicle_id] = shard_id
        
        # 更新分片的车辆数量
        if shard_id in self.shards:
            self.shards[shard_id]["vehicle_count"] += 1
        
        return shard_id
    
    def get_node_for_shard(self, shard_id: str) -> Optional[str]:
        """
        获取负责分片的节点
        
        Args:
            shard_id: 分片ID
            
        Returns:
            节点ID，如果分片未分配则返回None
        """
        if shard_id in self.shards:
            return self.shards[shard_id]["node_id"]
        return None
    
    def get_node_for_vehicle(self, vehicle_id: str, position: Position) -> Optional[str]:
        """
        获取负责车辆的节点
        
        Args:
            vehicle_id: 车辆ID
            position: 车辆位置
            
        Returns:
            节点ID，如果找不到则返回None
        """
        shard_id = self.get_shard_for_vehicle(vehicle_id, position)
        return self.get_node_for_shard(shard_id)
    
    def assign_shard(self, shard_id: str, node_id: str) -> bool:
        """
        分配分片给节点
        
        Args:
            shard_id: 分片ID
            node_id: 节点ID
            
        Returns:
            是否成功分配
        """
        if shard_id not in self.shards:
            logger.warning(f"Shard {shard_id} does not exist")
            return False
        
        if node_id not in self.node_shards:
            logger.warning(f"Node {node_id} is not registered")
            return False
        
        # 获取当前分配的节点
        current_node = self.shards[shard_id]["node_id"]
        
        # 如果已经分配给该节点，不需要重新分配
        if current_node == node_id:
            return True
        
        # 如果分片已经分配给其他节点，先从其他节点移除
        if current_node and current_node in self.node_shards:
            self.node_shards[current_node].discard(shard_id)
        
        # 分配给新节点
        self.shards[shard_id]["node_id"] = node_id
        self.node_shards[node_id].add(shard_id)
        
        # 发送分片分配消息
        asyncio.create_task(self._announce_shard_assignment(shard_id, node_id, current_node))
        
        logger.info(f"Assigned shard {shard_id} to node {node_id}")
        return True
    
    def release_shard(self, shard_id: str, node_id: str) -> bool:
        """
        从节点释放分片
        
        Args:
            shard_id: 分片ID
            node_id: 节点ID
            
        Returns:
            是否成功释放
        """
        if shard_id not in self.shards:
            logger.warning(f"Shard {shard_id} does not exist")
            return False
        
        if node_id not in self.node_shards:
            logger.warning(f"Node {node_id} is not registered")
            return False
        
        # 检查分片是否分配给该节点
        if self.shards[shard_id]["node_id"] != node_id:
            logger.warning(f"Shard {shard_id} is not assigned to node {node_id}")
            return False
        
        # 释放分片
        self.shards[shard_id]["node_id"] = None
        self.node_shards[node_id].discard(shard_id)
        
        # 发送分片释放消息
        asyncio.create_task(self._announce_shard_release(shard_id, node_id))
        
        logger.info(f"Released shard {shard_id} from node {node_id}")
        return True
    
    def on_shard_assignment(self, callback: Callable[[str, str], None]) -> None:
        """
        注册分片分配回调
        
        Args:
            callback: 回调函数 (shard_id, node_id) -> None
        """
        self.shard_assignment_callbacks.append(callback)
    
    def on_shard_release(self, callback: Callable[[str, str], None]) -> None:
        """
        注册分片释放回调
        
        Args:
            callback: 回调函数 (shard_id, node_id) -> None
        """
        self.shard_release_callbacks.append(callback)
    
    async def _announce_shard_assignment(self, shard_id: str, node_id: str, old_node_id: Optional[str]) -> None:
        """
        发布分片分配消息
        
        Args:
            shard_id: 分片ID
            node_id: 节点ID
            old_node_id: 旧节点ID
        """
        message = {
            "type": "shard_assignment",
            "shard_id": shard_id,
            "node_id": node_id,
            "old_node_id": old_node_id,
            "timestamp": time.time()
        }
        
        await self.shard_producer.send(message, key=shard_id)
        
        # 调用回调函数
        for callback in self.shard_assignment_callbacks:
            try:
                callback(shard_id, node_id)
            except Exception as e:
                logger.error(f"Error in shard assignment callback: {e}")
    
    async def _announce_shard_release(self, shard_id: str, node_id: str) -> None:
        """
        发布分片释放消息
        
        Args:
            shard_id: 分片ID
            node_id: 节点ID
        """
        message = {
            "type": "shard_release",
            "shard_id": shard_id,
            "node_id": node_id,
            "timestamp": time.time()
        }
        
        await self.shard_producer.send(message, key=shard_id)
        
        # 调用回调函数
        for callback in self.shard_release_callbacks:
            try:
                callback(shard_id, node_id)
            except Exception as e:
                logger.error(f"Error in shard release callback: {e}")
    
    def _handle_shard_message(self, message) -> None:
        """
        处理分片管理消息
        
        Args:
            message: 消息对象
        """
        try:
            data = message.value
            msg_type = data["type"]
            
            if msg_type == "shard_assignment":
                self._handle_shard_assignment(data)
            elif msg_type == "shard_release":
                self._handle_shard_release(data)
            elif msg_type == "node_failure":
                self._handle_node_failure(data)
        except Exception as e:
            logger.error(f"Error handling shard message: {e}")
    
    def _handle_shard_assignment(self, data) -> None:
        """
        处理分片分配消息
        
        Args:
            data: 消息数据
        """
        shard_id = data["shard_id"]
        node_id = data["node_id"]
        old_node_id = data["old_node_id"]
        
        # 更新本地状态
        if shard_id in self.shards:
            self.shards[shard_id]["node_id"] = node_id
        
        # 更新节点分片集合
        if node_id and node_id in self.node_shards:
            self.node_shards[node_id].add(shard_id)
        
        if old_node_id and old_node_id in self.node_shards:
            self.node_shards[old_node_id].discard(shard_id)
        
        # 调用回调函数
        for callback in self.shard_assignment_callbacks:
            try:
                callback(shard_id, node_id)
            except Exception as e:
                logger.error(f"Error in shard assignment callback: {e}")
    
    def _handle_shard_release(self, data) -> None:
        """
        处理分片释放消息
        
        Args:
            data: 消息数据
        """
        shard_id = data["shard_id"]
        node_id = data["node_id"]
        
        # 更新本地状态
        if shard_id in self.shards:
            self.shards[shard_id]["node_id"] = None
        
        # 更新节点分片集合
        if node_id and node_id in self.node_shards:
            self.node_shards[node_id].discard(shard_id)
        
        # 调用回调函数
        for callback in self.shard_release_callbacks:
            try:
                callback(shard_id, node_id)
            except Exception as e:
                logger.error(f"Error in shard release callback: {e}")
    
    def _handle_node_failure(self, data) -> None:
        """
        处理节点故障消息
        
        Args:
            data: 消息数据
        """
        failed_node = data["node_id"]
        
        # 重新分配该节点的分片
        if failed_node in self.node_shards:
            shards_to_reassign = list(self.node_shards[failed_node])
            for shard_id in shards_to_reassign:
                self._reassign_shard(shard_id, failed_node)
    
    def _reassign_shard(self, shard_id: str, old_node_id: str) -> None:
        """
        重新分配分片
        
        Args:
            shard_id: 分片ID
            old_node_id: 旧节点ID
        """
        # 找出负载最低的节点
        available_nodes = [n for n in self.node_shards.keys() if n != old_node_id]
        
        if not available_nodes:
            logger.warning(f"No available nodes to reassign shard {shard_id}")
            return
        
        # 按负载排序
        sorted_nodes = sorted(
            available_nodes,
            key=lambda n: self.node_loads.get(n, LoadMetrics()).cpu_usage
        )
        
        # 分配给负载最低的节点
        new_node = sorted_nodes[0]
        self.assign_shard(shard_id, new_node)
    
    async def _management_loop(self) -> None:
        """分片管理循环"""
        while self.running:
            try:
                # 检查是否需要重新平衡
                await self._check_rebalance()
                
                # 等待下一个检查周期
                await asyncio.sleep(10.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in shard management loop: {e}")
                await asyncio.sleep(10.0)
    
    async def _check_rebalance(self) -> None:
        """检查是否需要重新平衡分片"""
        now = time.time()
        
        # 检查空间分区器是否需要重新平衡
        rebalanced = self.spatial_partitioner.check_rebalance()
        
        # 如果空间分区器重新平衡了，也重新平衡分片
        if rebalanced:
            await self._rebalance_shards()
    
    async def _rebalance_shards(self) -> None:
        """重新平衡分片"""
        with Timer() as timer:
            # 计算每个节点的负载
            node_loads = {}
            for node_id, shards in self.node_shards.items():
                # 使用CPU使用率作为负载指标
                load = self.node_loads.get(node_id, LoadMetrics()).cpu_usage
                node_loads[node_id] = load
            
            # 找出负载过高和过低的节点
            avg_load = sum(node_loads.values()) / max(1, len(node_loads))
            overloaded = [n for n, l in node_loads.items() if l > avg_load * 1.2]
            underloaded = [n for n, l in node_loads.items() if l < avg_load * 0.8]
            
            # 从负载过高的节点移动分片到负载过低的节点
            moves = 0
            for over_node in overloaded:
                if not underloaded:
                    break
                
                # 获取该节点的分片
                shards = list(self.node_shards.get(over_node, set()))
                
                # 按负载排序
                sorted_shards = sorted(
                    shards,
                    key=lambda s: self.shards.get(s, {}).get("vehicle_count", 0),
                    reverse=True
                )
                
                # 移动分片
                for shard_id in sorted_shards:
                    if not underloaded:
                        break
                    
                    under_node = underloaded[0]
                    
                    # 分配给负载低的节点
                    self.assign_shard(shard_id, under_node)
                    moves += 1
                    
                    # 更新负载
                    shard_load = self.shards.get(shard_id, {}).get("load", 0.0)
                    node_loads[over_node] -= shard_load
                    node_loads[under_node] += shard_load
                    
                    # 检查是否仍然负载过低
                    if node_loads[under_node] >= avg_load * 0.8:
                        underloaded.remove(under_node)
        
        logger.info(f"Shard rebalance completed in {timer.elapsed_ms:.2f}ms, moved {moves} shards")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取分片管理器统计信息
        
        Returns:
            统计信息字典
        """
        # 计算每个节点的分片数量和车辆数量
        node_stats = {}
        for node_id, shards in self.node_shards.items():
            vehicle_count = sum(self.shards.get(s, {}).get("vehicle_count", 0) for s in shards)
            
            node_stats[node_id] = {
                "shards": len(shards),
                "vehicles": vehicle_count,
                "load": self.node_loads.get(node_id, LoadMetrics()).cpu_usage
            }
        
        return {
            "total_shards": len(self.shards),
            "total_vehicles": len(self.vehicle_to_shard),
            "nodes": node_stats
        }


class LoadBalancer:
    """负载均衡器，负责均衡计算节点的负载"""
    
    def __init__(self, broker: MessageBroker, node_id: str, 
                shard_manager: ShardManager,
                check_interval: float = 30.0):
        """
        初始化负载均衡器
        
        Args:
            broker: 消息代理
            node_id: 节点ID
            shard_manager: 分片管理器
            check_interval: 检查间隔（秒）
        """
        self.broker = broker
        self.node_id = node_id
        self.shard_manager = shard_manager
        self.check_interval = check_interval
        
        # 节点信息
        self.nodes: Dict[str, NodeInfo] = {}  # node_id -> NodeInfo
        
        # 负载信息
        self.node_loads: Dict[str, LoadMetrics] = {}  # node_id -> LoadMetrics
        
        # 消息通信
        self.load_producer = MessageProducer(broker, default_topic="load-balancing")
        self.load_consumer = MessageConsumer(
            broker=broker,
            topics=["load-balancing"],
            group_id=f"load-balancer-{node_id}"
        )
        
        # 任务
        self.running = False
        self.balancing_task = None
        
        logger.info(f"Load balancer initialized for node {node_id}")
    
    async def start(self) -> None:
        """启动负载均衡器"""
        if self.running:
            return
        
        self.running = True
        
        # 启动消费者
        await self.load_consumer.start()
        self.load_consumer.on_message("load-balancing", self._handle_load_message)
        
        # 启动均衡任务
        self.balancing_task = asyncio.create_task(self._balancing_loop())
        
        logger.info(f"Load balancer started for node {self.node_id}")
    
    async def stop(self) -> None:
        """停止负载均衡器"""
        if not self.running:
            return
        
        self.running = False
        
        # 停止均衡任务
        if self.balancing_task:
            self.balancing_task.cancel()
            try:
                await self.balancing_task
            except asyncio.CancelledError:
                pass
        
        # 停止消费者
        await self.load_consumer.stop()
        
        logger.info(f"Load balancer stopped for node {self.node_id}")
    
    def register_node(self, node_info: NodeInfo) -> None:
        """
        注册节点
        
        Args:
            node_info: 节点信息
        """
        self.nodes[node_info.id] = node_info
        
        # 同时在分片管理器中注册
        self.shard_manager.register_node(node_info.id)
        
        logger.info(f"Registered node {node_info.id} with capacity {node_info.capacity}")
    
    def unregister_node(self, node_id: str) -> None:
        """
        注销节点
        
        Args:
            node_id: 节点ID
        """
        if node_id in self.nodes:
            del self.nodes[node_id]
        
        if node_id in self.node_loads:
            del self.node_loads[node_id]
        
        # 同时在分片管理器中注销
        self.shard_manager.unregister_node(node_id)
        
        logger.info(f"Unregistered node {node_id}")
    
    def update_load(self, node_id: str, load_metrics: LoadMetrics) -> None:
        """
        更新节点负载
        
        Args:
            node_id: 节点ID
            load_metrics: 负载指标
        """
        self.node_loads[node_id] = load_metrics
        
        # 同时更新分片管理器中的负载
        self.shard_manager.update_node_load(node_id, load_metrics)
        
        # 发布负载更新消息
        asyncio.create_task(self._announce_load_update(node_id, load_metrics))
    
    async def _announce_load_update(self, node_id: str, load_metrics: LoadMetrics) -> None:
        """
        发布负载更新消息
        
        Args:
            node_id: 节点ID
            load_metrics: 负载指标
        """
        message = {
            "type": "load_update",
            "node_id": node_id,
            "load_metrics": load_metrics.__dict__,
            "timestamp": time.time()
        }
        
        await self.load_producer.send(message, key=node_id)
    
    def _handle_load_message(self, message) -> None:
        """
        处理负载消息
        
        Args:
            message: 消息对象
        """
        try:
            data = message.value
            msg_type = data["type"]
            
            if msg_type == "load_update":
                self._handle_load_update(data)
        except Exception as e:
            logger.error(f"Error handling load message: {e}")
    
    def _handle_load_update(self, data) -> None:
        """
        处理负载更新消息
        
        Args:
            data: 消息数据
        """
        node_id = data["node_id"]
        load_metrics_dict = data["load_metrics"]
        
        # 转换为LoadMetrics对象
        load_metrics = LoadMetrics(
            cpu_usage=load_metrics_dict["cpu_usage"],
            memory_usage=load_metrics_dict["memory_usage"],
            network_usage=load_metrics_dict["network_usage"],
            disk_usage=load_metrics_dict["disk_usage"],
            task_queue_size=load_metrics_dict["task_queue_size"],
            processing_rate=load_metrics_dict["processing_rate"],
            average_latency=load_metrics_dict["average_latency"]
        )
        
        # 更新负载信息
        self.node_loads[node_id] = load_metrics
        
        # 同时更新分片管理器中的负载
        self.shard_manager.update_node_load(node_id, load_metrics)
    
    async def _balancing_loop(self) -> None:
        """负载均衡循环"""
        while self.running:
            try:
                # 检查负载均衡
                await self._check_balance()
                
                # 等待下一个检查周期
                await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in load balancing loop: {e}")
                await asyncio.sleep(10.0)
    
    async def _check_balance(self) -> None:
        """检查负载均衡"""
        # 计算平均负载
        if not self.node_loads:
            return
        
        avg_cpu = sum(load.cpu_usage for load in self.node_loads.values()) / len(self.node_loads)
        avg_memory = sum(load.memory_usage for load in self.node_loads.values()) / len(self.node_loads)
        
        # 找出负载过高和过低的节点
        overloaded = []
        underloaded = []
        
        for node_id, load in self.node_loads.items():
            # 使用CPU和内存使用率作为主要指标
            if load.cpu_usage > avg_cpu * 1.2 or load.memory_usage > avg_memory * 1.2:
                overloaded.append(node_id)
            elif load.cpu_usage < avg_cpu * 0.8 and load.memory_usage < avg_memory * 0.8:
                underloaded.append(node_id)
        
        # 如果有负载不均衡，触发分片重新平衡
        if overloaded and underloaded:
            logger.info(f"Load imbalance detected: {len(overloaded)} overloaded, {len(underloaded)} underloaded")
            await self.shard_manager._rebalance_shards()
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取负载均衡器统计信息
        
        Returns:
            统计信息字典
        """
        # 计算平均负载
        avg_cpu = 0.0
        avg_memory = 0.0
        
        if self.node_loads:
            avg_cpu = sum(load.cpu_usage for load in self.node_loads.values()) / len(self.node_loads)
            avg_memory = sum(load.memory_usage for load in self.node_loads.values()) / len(self.node_loads)
        
        # 计算负载标准差
        cpu_std = 0.0
        memory_std = 0.0
        
        if self.node_loads:
            cpu_std = math.sqrt(sum((load.cpu_usage - avg_cpu) ** 2 for load in self.node_loads.values()) / len(self.node_loads))
            memory_std = math.sqrt(sum((load.memory_usage - avg_memory) ** 2 for load in self.node_loads.values()) / len(self.node_loads))
        
        return {
            "total_nodes": len(self.nodes),
            "active_nodes": len(self.node_loads),
            "avg_cpu_usage": avg_cpu,
            "avg_memory_usage": avg_memory,
            "cpu_std": cpu_std,
            "memory_std": memory_std
        }
