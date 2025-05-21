"""
性能监控工具，用于监控系统性能指标。
"""
import time
import os
import json
import csv
import argparse
import asyncio
import datetime
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field

import aiohttp
import psutil
import matplotlib.pyplot as plt
import numpy as np


@dataclass
class SystemMetrics:
    """系统指标"""
    timestamp: float
    cpu_usage: float
    memory_usage: float
    disk_io_read: float
    disk_io_write: float
    network_io_sent: float
    network_io_recv: float
    process_count: int
    thread_count: int
    open_files: int


@dataclass
class NodeMetrics:
    """节点指标"""
    node_id: str
    timestamp: float
    status: str
    cpu_usage: float
    memory_usage: float
    task_queue_size: int
    processing_rate: float
    average_latency: float
    error_rate: float
    uptime: float


@dataclass
class QueueMetrics:
    """队列指标"""
    queue_name: str
    timestamp: float
    queue_size: int
    enqueue_rate: float
    dequeue_rate: float
    average_wait_time: float


@dataclass
class DatabaseMetrics:
    """数据库指标"""
    db_name: str
    timestamp: float
    query_count: int
    query_rate: float
    average_query_time: float
    connection_count: int
    transaction_count: int


@dataclass
class CollisionMetrics:
    """碰撞检测指标"""
    timestamp: float
    vehicle_count: int
    detection_count: int
    detection_rate: float
    average_detection_time: float
    collision_count: int
    warning_count: int
    false_positive_rate: float


class PerformanceMonitor:
    """性能监控器，监控系统性能指标"""
    
    def __init__(self, 
                 api_url: str = "http://localhost:8000",
                 output_dir: str = "./monitoring",
                 interval: float = 1.0):
        """
        初始化性能监控器
        
        Args:
            api_url: API URL
            output_dir: 输出目录
            interval: 监控间隔（秒）
        """
        self.api_url = api_url
        self.output_dir = output_dir
        self.interval = interval
        
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        # HTTP会话
        self.session = None
        
        # 指标历史
        self.system_metrics_history: List[SystemMetrics] = []
        self.node_metrics_history: Dict[str, List[NodeMetrics]] = {}
        self.queue_metrics_history: Dict[str, List[QueueMetrics]] = {}
        self.db_metrics_history: Dict[str, List[DatabaseMetrics]] = {}
        self.collision_metrics_history: List[CollisionMetrics] = []
        
        # 上次IO计数
        self.last_disk_io = None
        self.last_network_io = None
        self.last_io_time = time.time()
        
        # 监控任务
        self.monitoring_tasks = []
        self.running = False
    
    async def start(self) -> None:
        """启动监控"""
        if self.running:
            return
        
        self.running = True
        
        # 创建HTTP会话
        self.session = aiohttp.ClientSession()
        
        # 创建监控任务
        self.monitoring_tasks = [
            asyncio.create_task(self._monitor_system()),
            asyncio.create_task(self._monitor_nodes()),
            asyncio.create_task(self._monitor_queues()),
            asyncio.create_task(self._monitor_databases()),
            asyncio.create_task(self._monitor_collision_detection())
        ]
        
        print(f"Performance monitoring started with interval {self.interval}s")
    
    async def stop(self) -> None:
        """停止监控"""
        if not self.running:
            return
        
        self.running = False
        
        # 取消监控任务
        for task in self.monitoring_tasks:
            task.cancel()
        
        # 等待任务完成
        for task in self.monitoring_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        # 关闭HTTP会话
        if self.session:
            await self.session.close()
            self.session = None
        
        # 保存监控数据
        self._save_metrics()
        
        print("Performance monitoring stopped")
    
    async def _monitor_system(self) -> None:
        """监控系统指标"""
        while self.running:
            try:
                # 收集系统指标
                metrics = self._collect_system_metrics()
                
                # 添加到历史记录
                self.system_metrics_history.append(metrics)
                
                # 打印指标
                print(f"[System] CPU: {metrics.cpu_usage:.1f}%, "
                      f"Memory: {metrics.memory_usage:.1f}%, "
                      f"Disk IO: {metrics.disk_io_read:.1f}MB/s read, {metrics.disk_io_write:.1f}MB/s write, "
                      f"Network IO: {metrics.network_io_recv:.1f}MB/s recv, {metrics.network_io_sent:.1f}MB/s sent")
            except Exception as e:
                print(f"Error monitoring system: {e}")
            
            # 等待下一个间隔
            await asyncio.sleep(self.interval)
    
    async def _monitor_nodes(self) -> None:
        """监控节点指标"""
        while self.running:
            try:
                # 获取节点列表
                nodes = await self._get_nodes()
                
                for node_id in nodes:
                    # 获取节点指标
                    metrics = await self._get_node_metrics(node_id)
                    
                    # 添加到历史记录
                    if node_id not in self.node_metrics_history:
                        self.node_metrics_history[node_id] = []
                    
                    self.node_metrics_history[node_id].append(metrics)
                    
                    # 打印指标
                    print(f"[Node {node_id}] Status: {metrics.status}, "
                          f"CPU: {metrics.cpu_usage:.1f}%, "
                          f"Memory: {metrics.memory_usage:.1f}%, "
                          f"Queue: {metrics.task_queue_size}, "
                          f"Rate: {metrics.processing_rate:.1f} req/s, "
                          f"Latency: {metrics.average_latency:.2f}ms")
            except Exception as e:
                print(f"Error monitoring nodes: {e}")
            
            # 等待下一个间隔
            await asyncio.sleep(self.interval)
    
    async def _monitor_queues(self) -> None:
        """监控队列指标"""
        while self.running:
            try:
                # 获取队列列表
                queues = await self._get_queues()
                
                for queue_name in queues:
                    # 获取队列指标
                    metrics = await self._get_queue_metrics(queue_name)
                    
                    # 添加到历史记录
                    if queue_name not in self.queue_metrics_history:
                        self.queue_metrics_history[queue_name] = []
                    
                    self.queue_metrics_history[queue_name].append(metrics)
                    
                    # 打印指标
                    print(f"[Queue {queue_name}] Size: {metrics.queue_size}, "
                          f"Enqueue: {metrics.enqueue_rate:.1f} msg/s, "
                          f"Dequeue: {metrics.dequeue_rate:.1f} msg/s, "
                          f"Wait time: {metrics.average_wait_time:.2f}ms")
            except Exception as e:
                print(f"Error monitoring queues: {e}")
            
            # 等待下一个间隔
            await asyncio.sleep(self.interval)
    
    async def _monitor_databases(self) -> None:
        """监控数据库指标"""
        while self.running:
            try:
                # 获取数据库列表
                databases = await self._get_databases()
                
                for db_name in databases:
                    # 获取数据库指标
                    metrics = await self._get_database_metrics(db_name)
                    
                    # 添加到历史记录
                    if db_name not in self.db_metrics_history:
                        self.db_metrics_history[db_name] = []
                    
                    self.db_metrics_history[db_name].append(metrics)
                    
                    # 打印指标
                    print(f"[DB {db_name}] Queries: {metrics.query_count}, "
                          f"Rate: {metrics.query_rate:.1f} q/s, "
                          f"Query time: {metrics.average_query_time:.2f}ms, "
                          f"Connections: {metrics.connection_count}")
            except Exception as e:
                print(f"Error monitoring databases: {e}")
            
            # 等待下一个间隔
            await asyncio.sleep(self.interval)
    
    async def _monitor_collision_detection(self) -> None:
        """监控碰撞检测指标"""
        while self.running:
            try:
                # 获取碰撞检测指标
                metrics = await self._get_collision_metrics()
                
                # 添加到历史记录
                self.collision_metrics_history.append(metrics)
                
                # 打印指标
                print(f"[Collision] Vehicles: {metrics.vehicle_count}, "
                      f"Detections: {metrics.detection_count}, "
                      f"Rate: {metrics.detection_rate:.1f} det/s, "
                      f"Time: {metrics.average_detection_time:.2f}ms, "
                      f"Collisions: {metrics.collision_count}, "
                      f"Warnings: {metrics.warning_count}")
            except Exception as e:
                print(f"Error monitoring collision detection: {e}")
            
            # 等待下一个间隔
            await asyncio.sleep(self.interval)
    
    def _collect_system_metrics(self) -> SystemMetrics:
        """
        收集系统指标
        
        Returns:
            系统指标
        """
        # CPU使用率
        cpu_usage = psutil.cpu_percent()
        
        # 内存使用率
        memory_usage = psutil.virtual_memory().percent
        
        # 磁盘IO
        current_time = time.time()
        current_disk_io = psutil.disk_io_counters()
        
        if self.last_disk_io is None:
            disk_io_read = 0
            disk_io_write = 0
        else:
            time_diff = current_time - self.last_io_time
            if time_diff > 0:
                disk_io_read = (current_disk_io.read_bytes - self.last_disk_io.read_bytes) / (1024 * 1024 * time_diff)  # MB/s
                disk_io_write = (current_disk_io.write_bytes - self.last_disk_io.write_bytes) / (1024 * 1024 * time_diff)  # MB/s
            else:
                disk_io_read = 0
                disk_io_write = 0
        
        self.last_disk_io = current_disk_io
        
        # 网络IO
        current_network_io = psutil.net_io_counters()
        
        if self.last_network_io is None:
            network_io_sent = 0
            network_io_recv = 0
        else:
            time_diff = current_time - self.last_io_time
            if time_diff > 0:
                network_io_sent = (current_network_io.bytes_sent - self.last_network_io.bytes_sent) / (1024 * 1024 * time_diff)  # MB/s
                network_io_recv = (current_network_io.bytes_recv - self.last_network_io.bytes_recv) / (1024 * 1024 * time_diff)  # MB/s
            else:
                network_io_sent = 0
                network_io_recv = 0
        
        self.last_network_io = current_network_io
        self.last_io_time = current_time
        
        # 进程数
        process_count = len(psutil.pids())
        
        # 线程数
        thread_count = 0
        for proc in psutil.process_iter(['pid', 'num_threads']):
            try:
                thread_count += proc.info['num_threads']
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        
        # 打开文件数
        open_files = 0
        for proc in psutil.process_iter(['pid', 'open_files']):
            try:
                files = proc.info['open_files']
                if files:
                    open_files += len(files)
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        
        return SystemMetrics(
            timestamp=current_time,
            cpu_usage=cpu_usage,
            memory_usage=memory_usage,
            disk_io_read=disk_io_read,
            disk_io_write=disk_io_write,
            network_io_sent=network_io_sent,
            network_io_recv=network_io_recv,
            process_count=process_count,
            thread_count=thread_count,
            open_files=open_files
        )
    
    async def _get_nodes(self) -> List[str]:
        """
        获取节点列表
        
        Returns:
            节点ID列表
        """
        try:
            async with self.session.get(f"{self.api_url}/api/nodes") as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("nodes", [])
        except Exception as e:
            print(f"Error getting nodes: {e}")
        
        return []
    
    async def _get_node_metrics(self, node_id: str) -> NodeMetrics:
        """
        获取节点指标
        
        Args:
            node_id: 节点ID
            
        Returns:
            节点指标
        """
        try:
            async with self.session.get(f"{self.api_url}/api/nodes/{node_id}/metrics") as response:
                if response.status == 200:
                    data = await response.json()
                    return NodeMetrics(
                        node_id=node_id,
                        timestamp=time.time(),
                        status=data.get("status", "unknown"),
                        cpu_usage=data.get("cpu_usage", 0.0),
                        memory_usage=data.get("memory_usage", 0.0),
                        task_queue_size=data.get("task_queue_size", 0),
                        processing_rate=data.get("processing_rate", 0.0),
                        average_latency=data.get("average_latency", 0.0),
                        error_rate=data.get("error_rate", 0.0),
                        uptime=data.get("uptime", 0.0)
                    )
        except Exception as e:
            print(f"Error getting node metrics for {node_id}: {e}")
        
        # 返回默认指标
        return NodeMetrics(
            node_id=node_id,
            timestamp=time.time(),
            status="unknown",
            cpu_usage=0.0,
            memory_usage=0.0,
            task_queue_size=0,
            processing_rate=0.0,
            average_latency=0.0,
            error_rate=0.0,
            uptime=0.0
        )
    
    async def _get_queues(self) -> List[str]:
        """
        获取队列列表
        
        Returns:
            队列名称列表
        """
        try:
            async with self.session.get(f"{self.api_url}/api/queues") as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("queues", [])
        except Exception as e:
            print(f"Error getting queues: {e}")
        
        return []
    
    async def _get_queue_metrics(self, queue_name: str) -> QueueMetrics:
        """
        获取队列指标
        
        Args:
            queue_name: 队列名称
            
        Returns:
            队列指标
        """
        try:
            async with self.session.get(f"{self.api_url}/api/queues/{queue_name}/metrics") as response:
                if response.status == 200:
                    data = await response.json()
                    return QueueMetrics(
                        queue_name=queue_name,
                        timestamp=time.time(),
                        queue_size=data.get("queue_size", 0),
                        enqueue_rate=data.get("enqueue_rate", 0.0),
                        dequeue_rate=data.get("dequeue_rate", 0.0),
                        average_wait_time=data.get("average_wait_time", 0.0)
                    )
        except Exception as e:
            print(f"Error getting queue metrics for {queue_name}: {e}")
        
        # 返回默认指标
        return QueueMetrics(
            queue_name=queue_name,
            timestamp=time.time(),
            queue_size=0,
            enqueue_rate=0.0,
            dequeue_rate=0.0,
            average_wait_time=0.0
        )
    
    async def _get_databases(self) -> List[str]:
        """
        获取数据库列表
        
        Returns:
            数据库名称列表
        """
        try:
            async with self.session.get(f"{self.api_url}/api/databases") as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("databases", [])
        except Exception as e:
            print(f"Error getting databases: {e}")
        
        return []
    
    async def _get_database_metrics(self, db_name: str) -> DatabaseMetrics:
        """
        获取数据库指标
        
        Args:
            db_name: 数据库名称
            
        Returns:
            数据库指标
        """
        try:
            async with self.session.get(f"{self.api_url}/api/databases/{db_name}/metrics") as response:
                if response.status == 200:
                    data = await response.json()
                    return DatabaseMetrics(
                        db_name=db_name,
                        timestamp=time.time(),
                        query_count=data.get("query_count", 0),
                        query_rate=data.get("query_rate", 0.0),
                        average_query_time=data.get("average_query_time", 0.0),
                        connection_count=data.get("connection_count", 0),
                        transaction_count=data.get("transaction_count", 0)
                    )
        except Exception as e:
            print(f"Error getting database metrics for {db_name}: {e}")
        
        # 返回默认指标
        return DatabaseMetrics(
            db_name=db_name,
            timestamp=time.time(),
            query_count=0,
            query_rate=0.0,
            average_query_time=0.0,
            connection_count=0,
            transaction_count=0
        )
    
    async def _get_collision_metrics(self) -> CollisionMetrics:
        """
        获取碰撞检测指标
        
        Returns:
            碰撞检测指标
        """
        try:
            async with self.session.get(f"{self.api_url}/api/collision/metrics") as response:
                if response.status == 200:
                    data = await response.json()
                    return CollisionMetrics(
                        timestamp=time.time(),
                        vehicle_count=data.get("vehicle_count", 0),
                        detection_count=data.get("detection_count", 0),
                        detection_rate=data.get("detection_rate", 0.0),
                        average_detection_time=data.get("average_detection_time", 0.0),
                        collision_count=data.get("collision_count", 0),
                        warning_count=data.get("warning_count", 0),
                        false_positive_rate=data.get("false_positive_rate", 0.0)
                    )
        except Exception as e:
            print(f"Error getting collision metrics: {e}")
        
        # 返回默认指标
        return CollisionMetrics(
            timestamp=time.time(),
            vehicle_count=0,
            detection_count=0,
            detection_rate=0.0,
            average_detection_time=0.0,
            collision_count=0,
            warning_count=0,
            false_positive_rate=0.0
        )
    
    def _save_metrics(self) -> None:
        """保存监控数据"""
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 保存系统指标
        if self.system_metrics_history:
            filename = f"{self.output_dir}/system_metrics_{timestamp}.csv"
            with open(filename, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    "timestamp", "cpu_usage", "memory_usage", "disk_io_read", "disk_io_write",
                    "network_io_sent", "network_io_recv", "process_count", "thread_count", "open_files"
                ])
                for metrics in self.system_metrics_history:
                    writer.writerow([
                        metrics.timestamp, metrics.cpu_usage, metrics.memory_usage,
                        metrics.disk_io_read, metrics.disk_io_write,
                        metrics.network_io_sent, metrics.network_io_recv,
                        metrics.process_count, metrics.thread_count, metrics.open_files
                    ])
            print(f"System metrics saved to {filename}")
        
        # 保存节点指标
        for node_id, metrics_list in self.node_metrics_history.items():
            if metrics_list:
                filename = f"{self.output_dir}/node_{node_id}_metrics_{timestamp}.csv"
                with open(filename, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        "timestamp", "status", "cpu_usage", "memory_usage",
                        "task_queue_size", "processing_rate", "average_latency",
                        "error_rate", "uptime"
                    ])
                    for metrics in metrics_list:
                        writer.writerow([
                            metrics.timestamp, metrics.status, metrics.cpu_usage, metrics.memory_usage,
                            metrics.task_queue_size, metrics.processing_rate, metrics.average_latency,
                            metrics.error_rate, metrics.uptime
                        ])
                print(f"Node {node_id} metrics saved to {filename}")
        
        # 保存队列指标
        for queue_name, metrics_list in self.queue_metrics_history.items():
            if metrics_list:
                filename = f"{self.output_dir}/queue_{queue_name}_metrics_{timestamp}.csv"
                with open(filename, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        "timestamp", "queue_size", "enqueue_rate", "dequeue_rate", "average_wait_time"
                    ])
                    for metrics in metrics_list:
                        writer.writerow([
                            metrics.timestamp, metrics.queue_size, metrics.enqueue_rate,
                            metrics.dequeue_rate, metrics.average_wait_time
                        ])
                print(f"Queue {queue_name} metrics saved to {filename}")
        
        # 保存数据库指标
        for db_name, metrics_list in self.db_metrics_history.items():
            if metrics_list:
                filename = f"{self.output_dir}/db_{db_name}_metrics_{timestamp}.csv"
                with open(filename, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        "timestamp", "query_count", "query_rate", "average_query_time",
                        "connection_count", "transaction_count"
                    ])
                    for metrics in metrics_list:
                        writer.writerow([
                            metrics.timestamp, metrics.query_count, metrics.query_rate,
                            metrics.average_query_time, metrics.connection_count, metrics.transaction_count
                        ])
                print(f"Database {db_name} metrics saved to {filename}")
        
        # 保存碰撞检测指标
        if self.collision_metrics_history:
            filename = f"{self.output_dir}/collision_metrics_{timestamp}.csv"
            with open(filename, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    "timestamp", "vehicle_count", "detection_count", "detection_rate",
                    "average_detection_time", "collision_count", "warning_count", "false_positive_rate"
                ])
                for metrics in self.collision_metrics_history:
                    writer.writerow([
                        metrics.timestamp, metrics.vehicle_count, metrics.detection_count,
                        metrics.detection_rate, metrics.average_detection_time,
                        metrics.collision_count, metrics.warning_count, metrics.false_positive_rate
                    ])
            print(f"Collision metrics saved to {filename}")
        
        # 生成图表
        self._generate_charts(timestamp)
    
    def _generate_charts(self, timestamp: str) -> None:
        """
        生成图表
        
        Args:
            timestamp: 时间戳
        """
        # 系统指标图表
        if self.system_metrics_history:
            # 提取数据
            timestamps = [(m.timestamp - self.system_metrics_history[0].timestamp) for m in self.system_metrics_history]
            cpu_usages = [m.cpu_usage for m in self.system_metrics_history]
            memory_usages = [m.memory_usage for m in self.system_metrics_history]
            disk_io_reads = [m.disk_io_read for m in self.system_metrics_history]
            disk_io_writes = [m.disk_io_write for m in self.system_metrics_history]
            network_io_sents = [m.network_io_sent for m in self.system_metrics_history]
            network_io_recvs = [m.network_io_recv for m in self.system_metrics_history]
            
            # CPU和内存使用率
            plt.figure(figsize=(10, 6))
            plt.plot(timestamps, cpu_usages, label='CPU')
            plt.plot(timestamps, memory_usages, label='Memory')
            plt.title('CPU and Memory Usage')
            plt.xlabel('Time (seconds)')
            plt.ylabel('Usage (%)')
            plt.legend()
            plt.grid(True)
            plt.savefig(f"{self.output_dir}/system_cpu_memory_{timestamp}.png")
            plt.close()
            
            # 磁盘IO
            plt.figure(figsize=(10, 6))
            plt.plot(timestamps, disk_io_reads, label='Read')
            plt.plot(timestamps, disk_io_writes, label='Write')
            plt.title('Disk I/O')
            plt.xlabel('Time (seconds)')
            plt.ylabel('I/O (MB/s)')
            plt.legend()
            plt.grid(True)
            plt.savefig(f"{self.output_dir}/system_disk_io_{timestamp}.png")
            plt.close()
            
            # 网络IO
            plt.figure(figsize=(10, 6))
            plt.plot(timestamps, network_io_sents, label='Sent')
            plt.plot(timestamps, network_io_recvs, label='Received')
            plt.title('Network I/O')
            plt.xlabel('Time (seconds)')
            plt.ylabel('I/O (MB/s)')
            plt.legend()
            plt.grid(True)
            plt.savefig(f"{self.output_dir}/system_network_io_{timestamp}.png")
            plt.close()
        
        # 节点指标图表
        for node_id, metrics_list in self.node_metrics_history.items():
            if metrics_list:
                # 提取数据
                timestamps = [(m.timestamp - metrics_list[0].timestamp) for m in metrics_list]
                cpu_usages = [m.cpu_usage for m in metrics_list]
                memory_usages = [m.memory_usage for m in metrics_list]
                queue_sizes = [m.task_queue_size for m in metrics_list]
                processing_rates = [m.processing_rate for m in metrics_list]
                latencies = [m.average_latency for m in metrics_list]
                error_rates = [m.error_rate for m in metrics_list]
                
                # CPU和内存使用率
                plt.figure(figsize=(10, 6))
                plt.plot(timestamps, cpu_usages, label='CPU')
                plt.plot(timestamps, memory_usages, label='Memory')
                plt.title(f'Node {node_id} - CPU and Memory Usage')
                plt.xlabel('Time (seconds)')
                plt.ylabel('Usage (%)')
                plt.legend()
                plt.grid(True)
                plt.savefig(f"{self.output_dir}/node_{node_id}_cpu_memory_{timestamp}.png")
                plt.close()
                
                # 队列大小和处理速率
                fig, ax1 = plt.subplots(figsize=(10, 6))
                ax1.set_xlabel('Time (seconds)')
                ax1.set_ylabel('Queue Size', color='tab:blue')
                ax1.plot(timestamps, queue_sizes, color='tab:blue', label='Queue Size')
                ax1.tick_params(axis='y', labelcolor='tab:blue')
                
                ax2 = ax1.twinx()
                ax2.set_ylabel('Processing Rate (req/s)', color='tab:red')
                ax2.plot(timestamps, processing_rates, color='tab:red', label='Processing Rate')
                ax2.tick_params(axis='y', labelcolor='tab:red')
                
                plt.title(f'Node {node_id} - Queue Size and Processing Rate')
                fig.tight_layout()
                plt.savefig(f"{self.output_dir}/node_{node_id}_queue_rate_{timestamp}.png")
                plt.close()
                
                # 延迟和错误率
                fig, ax1 = plt.subplots(figsize=(10, 6))
                ax1.set_xlabel('Time (seconds)')
                ax1.set_ylabel('Latency (ms)', color='tab:blue')
                ax1.plot(timestamps, latencies, color='tab:blue', label='Latency')
                ax1.tick_params(axis='y', labelcolor='tab:blue')
                
                ax2 = ax1.twinx()
                ax2.set_ylabel('Error Rate (%)', color='tab:red')
                ax2.plot(timestamps, error_rates, color='tab:red', label='Error Rate')
                ax2.tick_params(axis='y', labelcolor='tab:red')
                
                plt.title(f'Node {node_id} - Latency and Error Rate')
                fig.tight_layout()
                plt.savefig(f"{self.output_dir}/node_{node_id}_latency_error_{timestamp}.png")
                plt.close()
        
        # 碰撞检测指标图表
        if self.collision_metrics_history:
            # 提取数据
            timestamps = [(m.timestamp - self.collision_metrics_history[0].timestamp) for m in self.collision_metrics_history]
            vehicle_counts = [m.vehicle_count for m in self.collision_metrics_history]
            detection_rates = [m.detection_rate for m in self.collision_metrics_history]
            detection_times = [m.average_detection_time for m in self.collision_metrics_history]
            collision_counts = [m.collision_count for m in self.collision_metrics_history]
            warning_counts = [m.warning_count for m in self.collision_metrics_history]
            
            # 车辆数量和检测速率
            fig, ax1 = plt.subplots(figsize=(10, 6))
            ax1.set_xlabel('Time (seconds)')
            ax1.set_ylabel('Vehicle Count', color='tab:blue')
            ax1.plot(timestamps, vehicle_counts, color='tab:blue', label='Vehicle Count')
            ax1.tick_params(axis='y', labelcolor='tab:blue')
            
            ax2 = ax1.twinx()
            ax2.set_ylabel('Detection Rate (det/s)', color='tab:red')
            ax2.plot(timestamps, detection_rates, color='tab:red', label='Detection Rate')
            ax2.tick_params(axis='y', labelcolor='tab:red')
            
            plt.title('Vehicle Count and Detection Rate')
            fig.tight_layout()
            plt.savefig(f"{self.output_dir}/collision_vehicle_rate_{timestamp}.png")
            plt.close()
            
            # 检测时间
            plt.figure(figsize=(10, 6))
            plt.plot(timestamps, detection_times)
            plt.title('Average Detection Time')
            plt.xlabel('Time (seconds)')
            plt.ylabel('Time (ms)')
            plt.grid(True)
            plt.savefig(f"{self.output_dir}/collision_detection_time_{timestamp}.png")
            plt.close()
            
            # 碰撞和预警数量
            plt.figure(figsize=(10, 6))
            plt.plot(timestamps, collision_counts, label='Collisions')
            plt.plot(timestamps, warning_counts, label='Warnings')
            plt.title('Collision and Warning Count')
            plt.xlabel('Time (seconds)')
            plt.ylabel('Count')
            plt.legend()
            plt.grid(True)
            plt.savefig(f"{self.output_dir}/collision_warning_count_{timestamp}.png")
            plt.close()


async def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="Performance Monitor")
    parser.add_argument("--api-url", type=str, default="http://localhost:8000", help="API URL")
    parser.add_argument("--output-dir", type=str, default="./monitoring", help="Output directory")
    parser.add_argument("--interval", type=float, default=1.0, help="Monitoring interval in seconds")
    parser.add_argument("--duration", type=int, default=0, help="Monitoring duration in seconds (0 for infinite)")
    args = parser.parse_args()
    
    # 创建性能监控器
    monitor = PerformanceMonitor(
        api_url=args.api_url,
        output_dir=args.output_dir,
        interval=args.interval
    )
    
    try:
        # 启动监控
        await monitor.start()
        
        # 运行指定时间
        if args.duration > 0:
            await asyncio.sleep(args.duration)
        else:
            # 无限运行，直到用户中断
            while True:
                await asyncio.sleep(1.0)
    except KeyboardInterrupt:
        print("Monitoring stopped by user")
    finally:
        # 停止监控
        await monitor.stop()


if __name__ == "__main__":
    asyncio.run(main())
