"""
负载生成器，用于生成测试负载并测量系统性能。
"""
import time
import uuid
import random
import argparse
import asyncio
import json
import csv
import os
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
import statistics

import numpy as np
import matplotlib.pyplot as plt
from kafka import KafkaProducer, KafkaConsumer
import redis
import aiohttp
import psutil


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


class LoadGenerator:
    """负载生成器，生成测试负载并测量系统性能"""
    
    def __init__(self, 
                 target_url: str = "http://localhost:8000",
                 kafka_servers: str = "localhost:9092",
                 kafka_topic: str = "vehicle-positions",
                 redis_host: str = "localhost",
                 redis_port: int = 6379,
                 redis_channel: str = "vehicle-positions",
                 output_dir: str = "./results"):
        """
        初始化负载生成器
        
        Args:
            target_url: 目标URL
            kafka_servers: Kafka服务器地址
            kafka_topic: Kafka主题
            redis_host: Redis主机
            redis_port: Redis端口
            redis_channel: Redis通道
            output_dir: 输出目录
        """
        self.target_url = target_url
        self.kafka_servers = kafka_servers
        self.kafka_topic = kafka_topic
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_channel = redis_channel
        self.output_dir = output_dir
        
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        # Kafka生产者
        self.kafka_producer = None
        
        # Redis客户端
        self.redis_client = None
        
        # HTTP会话
        self.http_session = None
        
        # 性能指标
        self.latencies = []
        self.start_time = 0
        self.end_time = 0
        self.request_count = 0
        self.error_count = 0
        self.metrics_history = []
    
    async def initialize(self) -> None:
        """初始化连接"""
        # 初始化Kafka生产者
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=self.kafka_servers,
            value_serializer=lambda v: v.encode('utf-8')
        )
        
        # 初始化Redis客户端
        self.redis_client = redis.Redis(host=self.redis_host, port=self.redis_port)
        
        # 初始化HTTP会话
        self.http_session = aiohttp.ClientSession()
    
    async def close(self) -> None:
        """关闭连接"""
        # 关闭Kafka生产者
        if self.kafka_producer:
            self.kafka_producer.flush()
            self.kafka_producer.close()
        
        # 关闭Redis客户端
        if self.redis_client:
            self.redis_client.close()
        
        # 关闭HTTP会话
        if self.http_session:
            await self.http_session.close()
    
    async def run_load_test(self, 
                           mode: str = "http",
                           duration: int = 60,
                           rate: int = 100,
                           ramp_up: int = 10,
                           data_file: Optional[str] = None) -> PerformanceMetrics:
        """
        运行负载测试
        
        Args:
            mode: 测试模式 ("http", "kafka", "redis")
            duration: 测试持续时间（秒）
            rate: 目标请求率（每秒请求数）
            ramp_up: 爬升时间（秒）
            data_file: 数据文件路径
            
        Returns:
            性能指标
        """
        print(f"Starting load test in {mode} mode")
        print(f"Target rate: {rate} requests/second")
        print(f"Duration: {duration} seconds")
        print(f"Ramp-up time: {ramp_up} seconds")
        
        # 重置指标
        self.latencies = []
        self.start_time = time.time()
        self.end_time = self.start_time
        self.request_count = 0
        self.error_count = 0
        self.metrics_history = []
        
        # 加载测试数据
        test_data = []
        if data_file:
            with open(data_file, 'r') as f:
                for line in f:
                    test_data.append(line.strip())
        else:
            # 生成随机测试数据
            for i in range(1000):
                test_data.append(self._generate_random_vehicle_data())
        
        # 创建任务
        tasks = []
        
        # 监控任务
        monitor_task = asyncio.create_task(self._monitor_metrics(1.0))
        tasks.append(monitor_task)
        
        # 负载生成任务
        load_task = asyncio.create_task(self._generate_load(mode, duration, rate, ramp_up, test_data))
        tasks.append(load_task)
        
        # 等待所有任务完成
        await asyncio.gather(*tasks)
        
        # 计算性能指标
        metrics = self._calculate_metrics()
        
        # 保存结果
        self._save_results(mode, rate, duration)
        
        return metrics
    
    async def _generate_load(self, mode: str, duration: int, rate: int, ramp_up: int, test_data: List[str]) -> None:
        """
        生成负载
        
        Args:
            mode: 测试模式
            duration: 测试持续时间（秒）
            rate: 目标请求率（每秒请求数）
            ramp_up: 爬升时间（秒）
            test_data: 测试数据
        """
        # 计算请求间隔
        interval = 1.0 / rate
        
        # 计算爬升步长
        if ramp_up > 0:
            step_count = 10  # 爬升步数
            step_duration = ramp_up / step_count
            step_rate = rate / step_count
        else:
            step_count = 1
            step_duration = 0
            step_rate = rate
        
        # 爬升阶段
        current_rate = 0
        for step in range(step_count):
            current_rate += step_rate
            step_interval = 1.0 / max(1, current_rate)
            
            step_end_time = time.time() + step_duration
            while time.time() < step_end_time:
                # 发送请求
                data_item = random.choice(test_data)
                await self._send_request(mode, data_item)
                
                # 等待下一个请求
                await asyncio.sleep(step_interval)
        
        # 持续阶段
        steady_end_time = time.time() + duration
        while time.time() < steady_end_time:
            # 发送请求
            data_item = random.choice(test_data)
            await self._send_request(mode, data_item)
            
            # 等待下一个请求
            await asyncio.sleep(interval)
        
        self.end_time = time.time()
    
    async def _send_request(self, mode: str, data: str) -> None:
        """
        发送请求
        
        Args:
            mode: 测试模式
            data: 请求数据
        """
        start_time = time.time()
        error = False
        
        try:
            if mode == "http":
                # HTTP请求
                async with self.http_session.post(f"{self.target_url}/api/vehicle", data=data) as response:
                    if response.status != 200:
                        error = True
            elif mode == "kafka":
                # Kafka消息
                self.kafka_producer.send(self.kafka_topic, data)
            elif mode == "redis":
                # Redis发布
                self.redis_client.publish(self.redis_channel, data)
        except Exception as e:
            print(f"Error sending request: {e}")
            error = True
        
        end_time = time.time()
        latency = (end_time - start_time) * 1000  # 毫秒
        
        # 更新指标
        self.latencies.append(latency)
        self.request_count += 1
        if error:
            self.error_count += 1
    
    async def _monitor_metrics(self, interval: float) -> None:
        """
        监控性能指标
        
        Args:
            interval: 监控间隔（秒）
        """
        while True:
            # 计算当前指标
            metrics = self._calculate_metrics()
            
            # 添加到历史记录
            self.metrics_history.append(metrics)
            
            # 打印当前指标
            elapsed = time.time() - self.start_time
            print(f"[{elapsed:.1f}s] Throughput: {metrics.throughput:.1f} req/s, "
                  f"Avg latency: {metrics.avg_latency:.2f} ms, "
                  f"P99 latency: {metrics.p99_latency:.2f} ms, "
                  f"Error rate: {metrics.error_rate:.2f}%, "
                  f"CPU: {metrics.cpu_usage:.1f}%, "
                  f"Memory: {metrics.memory_usage:.1f}%")
            
            # 检查是否结束
            if time.time() >= self.start_time + 3600:  # 最长运行1小时
                break
            
            # 等待下一个间隔
            await asyncio.sleep(interval)
    
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
        cpu_usage = psutil.cpu_percent()
        memory_usage = psutil.virtual_memory().percent
        
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
    
    def _save_results(self, mode: str, rate: int, duration: int) -> None:
        """
        保存测试结果
        
        Args:
            mode: 测试模式
            rate: 目标请求率
            duration: 测试持续时间
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_filename = f"{self.output_dir}/loadtest_{mode}_{rate}tps_{duration}s_{timestamp}"
        
        # 保存延迟数据
        with open(f"{base_filename}_latencies.csv", 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["latency_ms"])
            for latency in self.latencies:
                writer.writerow([latency])
        
        # 保存指标历史
        with open(f"{base_filename}_metrics.csv", 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                "timestamp", "throughput", "avg_latency", "p95_latency", "p99_latency",
                "max_latency", "error_rate", "cpu_usage", "memory_usage"
            ])
            for metrics in self.metrics_history:
                writer.writerow([
                    metrics.timestamp, metrics.throughput, metrics.avg_latency,
                    metrics.p95_latency, metrics.p99_latency, metrics.max_latency,
                    metrics.error_rate, metrics.cpu_usage, metrics.memory_usage
                ])
        
        # 生成摘要报告
        final_metrics = self._calculate_metrics()
        with open(f"{base_filename}_summary.txt", 'w') as f:
            f.write(f"Load Test Summary\n")
            f.write(f"================\n\n")
            f.write(f"Test Configuration:\n")
            f.write(f"  Mode: {mode}\n")
            f.write(f"  Target Rate: {rate} requests/second\n")
            f.write(f"  Duration: {duration} seconds\n")
            f.write(f"  Target URL: {self.target_url}\n\n")
            
            f.write(f"Test Results:\n")
            f.write(f"  Total Requests: {self.request_count}\n")
            f.write(f"  Total Errors: {self.error_count}\n")
            f.write(f"  Error Rate: {final_metrics.error_rate:.2f}%\n")
            f.write(f"  Actual Duration: {self.end_time - self.start_time:.2f} seconds\n")
            f.write(f"  Throughput: {final_metrics.throughput:.2f} requests/second\n\n")
            
            f.write(f"Latency (ms):\n")
            f.write(f"  Average: {final_metrics.avg_latency:.2f}\n")
            f.write(f"  P95: {final_metrics.p95_latency:.2f}\n")
            f.write(f"  P99: {final_metrics.p99_latency:.2f}\n")
            f.write(f"  Max: {final_metrics.max_latency:.2f}\n\n")
            
            f.write(f"Resource Usage:\n")
            f.write(f"  CPU: {final_metrics.cpu_usage:.2f}%\n")
            f.write(f"  Memory: {final_metrics.memory_usage:.2f}%\n")
        
        # 生成图表
        self._generate_charts(base_filename)
        
        print(f"Results saved to {base_filename}_*")
    
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
        plt.title('Throughput over Time')
        plt.xlabel('Time (seconds)')
        plt.ylabel('Throughput (requests/second)')
        plt.grid(True)
        plt.savefig(f"{base_filename}_throughput.png")
        plt.close()
        
        # 延迟图表
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps, avg_latencies, label='Average')
        plt.plot(timestamps, p95_latencies, label='P95')
        plt.plot(timestamps, p99_latencies, label='P99')
        plt.title('Latency over Time')
        plt.xlabel('Time (seconds)')
        plt.ylabel('Latency (ms)')
        plt.legend()
        plt.grid(True)
        plt.savefig(f"{base_filename}_latency.png")
        plt.close()
        
        # 资源使用图表
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps, cpu_usages, label='CPU')
        plt.plot(timestamps, memory_usages, label='Memory')
        plt.title('Resource Usage over Time')
        plt.xlabel('Time (seconds)')
        plt.ylabel('Usage (%)')
        plt.legend()
        plt.grid(True)
        plt.savefig(f"{base_filename}_resources.png")
        plt.close()
        
        # 延迟分布直方图
        plt.figure(figsize=(10, 6))
        plt.hist(self.latencies, bins=50)
        plt.title('Latency Distribution')
        plt.xlabel('Latency (ms)')
        plt.ylabel('Count')
        plt.grid(True)
        plt.savefig(f"{base_filename}_latency_hist.png")
        plt.close()
    
    def _generate_random_vehicle_data(self) -> str:
        """
        生成随机车辆数据
        
        Returns:
            JSON字符串
        """
        vehicle_id = f"vehicle-{random.randint(1, 10000)}"
        
        return json.dumps({
            "id": vehicle_id,
            "position": {
                "x": random.uniform(0, 10000),
                "y": random.uniform(0, 10000),
                "z": random.uniform(0, 100)
            },
            "velocity": {
                "x": random.uniform(-20, 20),
                "y": random.uniform(-20, 20),
                "z": random.uniform(-5, 5)
            },
            "acceleration": {
                "x": random.uniform(-2, 2),
                "y": random.uniform(-2, 2),
                "z": random.uniform(-1, 1)
            },
            "heading": random.uniform(0, 2 * 3.14159),
            "size": random.uniform(1, 5),
            "type": random.choice(["car", "truck", "bus", "motorcycle"]),
            "timestamp": time.time()
        })


class PerformanceAnalyzer:
    """性能分析器，分析测试结果"""
    
    def __init__(self, results_dir: str = "./results"):
        """
        初始化性能分析器
        
        Args:
            results_dir: 结果目录
        """
        self.results_dir = results_dir
    
    def analyze_test_results(self, test_files: List[str]) -> Dict[str, Any]:
        """
        分析测试结果
        
        Args:
            test_files: 测试文件列表
            
        Returns:
            分析结果
        """
        results = {}
        
        for file in test_files:
            if file.endswith("_metrics.csv"):
                # 分析指标文件
                test_name = os.path.basename(file).replace("_metrics.csv", "")
                metrics = self._analyze_metrics_file(file)
                results[test_name] = metrics
        
        return results
    
    def _analyze_metrics_file(self, file_path: str) -> Dict[str, Any]:
        """
        分析指标文件
        
        Args:
            file_path: 文件路径
            
        Returns:
            分析结果
        """
        timestamps = []
        throughputs = []
        avg_latencies = []
        p95_latencies = []
        p99_latencies = []
        max_latencies = []
        error_rates = []
        cpu_usages = []
        memory_usages = []
        
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                timestamps.append(float(row["timestamp"]))
                throughputs.append(float(row["throughput"]))
                avg_latencies.append(float(row["avg_latency"]))
                p95_latencies.append(float(row["p95_latency"]))
                p99_latencies.append(float(row["p99_latency"]))
                max_latencies.append(float(row["max_latency"]))
                error_rates.append(float(row["error_rate"]))
                cpu_usages.append(float(row["cpu_usage"]))
                memory_usages.append(float(row["memory_usage"]))
        
        # 计算统计数据
        avg_throughput = statistics.mean(throughputs) if throughputs else 0
        max_throughput = max(throughputs) if throughputs else 0
        avg_latency = statistics.mean(avg_latencies) if avg_latencies else 0
        avg_p95_latency = statistics.mean(p95_latencies) if p95_latencies else 0
        avg_p99_latency = statistics.mean(p99_latencies) if p99_latencies else 0
        max_latency = max(max_latencies) if max_latencies else 0
        avg_error_rate = statistics.mean(error_rates) if error_rates else 0
        avg_cpu_usage = statistics.mean(cpu_usages) if cpu_usages else 0
        avg_memory_usage = statistics.mean(memory_usages) if memory_usages else 0
        
        return {
            "avg_throughput": avg_throughput,
            "max_throughput": max_throughput,
            "avg_latency": avg_latency,
            "avg_p95_latency": avg_p95_latency,
            "avg_p99_latency": avg_p99_latency,
            "max_latency": max_latency,
            "avg_error_rate": avg_error_rate,
            "avg_cpu_usage": avg_cpu_usage,
            "avg_memory_usage": avg_memory_usage,
            "throughputs": throughputs,
            "avg_latencies": avg_latencies,
            "p95_latencies": p95_latencies,
            "p99_latencies": p99_latencies,
            "timestamps": timestamps
        }
    
    def compare_tests(self, test_results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        比较测试结果
        
        Args:
            test_results: 测试结果字典
            
        Returns:
            比较结果
        """
        comparison = {
            "throughput": {},
            "latency": {},
            "p99_latency": {},
            "error_rate": {},
            "resource_usage": {}
        }
        
        for test_name, results in test_results.items():
            comparison["throughput"][test_name] = results["avg_throughput"]
            comparison["latency"][test_name] = results["avg_latency"]
            comparison["p99_latency"][test_name] = results["avg_p99_latency"]
            comparison["error_rate"][test_name] = results["avg_error_rate"]
            comparison["resource_usage"][test_name] = {
                "cpu": results["avg_cpu_usage"],
                "memory": results["avg_memory_usage"]
            }
        
        return comparison
    
    def generate_comparison_report(self, comparison: Dict[str, Any], output_file: str) -> None:
        """
        生成比较报告
        
        Args:
            comparison: 比较结果
            output_file: 输出文件
        """
        with open(output_file, 'w') as f:
            f.write("Performance Test Comparison\n")
            f.write("==========================\n\n")
            
            # 吞吐量比较
            f.write("Throughput (requests/second):\n")
            for test_name, throughput in comparison["throughput"].items():
                f.write(f"  {test_name}: {throughput:.2f}\n")
            f.write("\n")
            
            # 延迟比较
            f.write("Average Latency (ms):\n")
            for test_name, latency in comparison["latency"].items():
                f.write(f"  {test_name}: {latency:.2f}\n")
            f.write("\n")
            
            # P99延迟比较
            f.write("P99 Latency (ms):\n")
            for test_name, latency in comparison["p99_latency"].items():
                f.write(f"  {test_name}: {latency:.2f}\n")
            f.write("\n")
            
            # 错误率比较
            f.write("Error Rate (%):\n")
            for test_name, error_rate in comparison["error_rate"].items():
                f.write(f"  {test_name}: {error_rate:.2f}\n")
            f.write("\n")
            
            # 资源使用比较
            f.write("Resource Usage:\n")
            for test_name, usage in comparison["resource_usage"].items():
                f.write(f"  {test_name}: CPU {usage['cpu']:.2f}%, Memory {usage['memory']:.2f}%\n")
            f.write("\n")
            
            # 结论
            f.write("Conclusions:\n")
            
            # 找出最佳吞吐量
            best_throughput_test = max(comparison["throughput"].items(), key=lambda x: x[1])[0]
            f.write(f"  Best throughput: {best_throughput_test}\n")
            
            # 找出最低延迟
            best_latency_test = min(comparison["latency"].items(), key=lambda x: x[1])[0]
            f.write(f"  Best latency: {best_latency_test}\n")
            
            # 找出最低P99延迟
            best_p99_test = min(comparison["p99_latency"].items(), key=lambda x: x[1])[0]
            f.write(f"  Best P99 latency: {best_p99_test}\n")
            
            # 找出最低错误率
            best_error_rate_test = min(comparison["error_rate"].items(), key=lambda x: x[1])[0]
            f.write(f"  Best error rate: {best_error_rate_test}\n")
    
    def generate_comparison_charts(self, test_results: Dict[str, Dict[str, Any]], output_prefix: str) -> None:
        """
        生成比较图表
        
        Args:
            test_results: 测试结果字典
            output_prefix: 输出文件前缀
        """
        # 吞吐量比较图
        plt.figure(figsize=(12, 6))
        for test_name, results in test_results.items():
            plt.plot(results["timestamps"], results["throughputs"], label=test_name)
        plt.title('Throughput Comparison')
        plt.xlabel('Time (seconds)')
        plt.ylabel('Throughput (requests/second)')
        plt.legend()
        plt.grid(True)
        plt.savefig(f"{output_prefix}_throughput_comparison.png")
        plt.close()
        
        # 延迟比较图
        plt.figure(figsize=(12, 6))
        for test_name, results in test_results.items():
            plt.plot(results["timestamps"], results["avg_latencies"], label=f"{test_name} (Avg)")
        plt.title('Average Latency Comparison')
        plt.xlabel('Time (seconds)')
        plt.ylabel('Latency (ms)')
        plt.legend()
        plt.grid(True)
        plt.savefig(f"{output_prefix}_latency_comparison.png")
        plt.close()
        
        # P99延迟比较图
        plt.figure(figsize=(12, 6))
        for test_name, results in test_results.items():
            plt.plot(results["timestamps"], results["p99_latencies"], label=test_name)
        plt.title('P99 Latency Comparison')
        plt.xlabel('Time (seconds)')
        plt.ylabel('P99 Latency (ms)')
        plt.legend()
        plt.grid(True)
        plt.savefig(f"{output_prefix}_p99_latency_comparison.png")
        plt.close()
        
        # 吞吐量vs延迟散点图
        plt.figure(figsize=(12, 6))
        for test_name, results in test_results.items():
            plt.scatter(results["throughputs"], results["p99_latencies"], label=test_name, alpha=0.7)
        plt.title('Throughput vs P99 Latency')
        plt.xlabel('Throughput (requests/second)')
        plt.ylabel('P99 Latency (ms)')
        plt.legend()
        plt.grid(True)
        plt.savefig(f"{output_prefix}_throughput_vs_latency.png")
        plt.close()


class FailureInjector:
    """故障注入器，用于测试系统可靠性"""
    
    def __init__(self, target_url: str = "http://localhost:8000"):
        """
        初始化故障注入器
        
        Args:
            target_url: 目标URL
        """
        self.target_url = target_url
        self.http_session = None
    
    async def initialize(self) -> None:
        """初始化连接"""
        self.http_session = aiohttp.ClientSession()
    
    async def close(self) -> None:
        """关闭连接"""
        if self.http_session:
            await self.http_session.close()
    
    async def inject_node_failure(self, node_id: str) -> bool:
        """
        注入节点故障
        
        Args:
            node_id: 节点ID
            
        Returns:
            是否成功
        """
        try:
            async with self.http_session.post(
                f"{self.target_url}/api/admin/inject-failure",
                json={"type": "node_failure", "node_id": node_id}
            ) as response:
                return response.status == 200
        except Exception as e:
            print(f"Error injecting node failure: {e}")
            return False
    
    async def inject_network_partition(self, node_ids: List[str]) -> bool:
        """
        注入网络分区
        
        Args:
            node_ids: 节点ID列表
            
        Returns:
            是否成功
        """
        try:
            async with self.http_session.post(
                f"{self.target_url}/api/admin/inject-failure",
                json={"type": "network_partition", "node_ids": node_ids}
            ) as response:
                return response.status == 200
        except Exception as e:
            print(f"Error injecting network partition: {e}")
            return False
    
    async def inject_high_load(self, duration: int = 60) -> bool:
        """
        注入高负载
        
        Args:
            duration: 持续时间（秒）
            
        Returns:
            是否成功
        """
        try:
            async with self.http_session.post(
                f"{self.target_url}/api/admin/inject-failure",
                json={"type": "high_load", "duration": duration}
            ) as response:
                return response.status == 200
        except Exception as e:
            print(f"Error injecting high load: {e}")
            return False
    
    async def inject_slow_response(self, latency: int = 500, duration: int = 60) -> bool:
        """
        注入慢响应
        
        Args:
            latency: 延迟（毫秒）
            duration: 持续时间（秒）
            
        Returns:
            是否成功
        """
        try:
            async with self.http_session.post(
                f"{self.target_url}/api/admin/inject-failure",
                json={"type": "slow_response", "latency": latency, "duration": duration}
            ) as response:
                return response.status == 200
        except Exception as e:
            print(f"Error injecting slow response: {e}")
            return False
    
    async def reset_failures(self) -> bool:
        """
        重置所有故障
        
        Returns:
            是否成功
        """
        try:
            async with self.http_session.post(
                f"{self.target_url}/api/admin/reset-failures"
            ) as response:
                return response.status == 200
        except Exception as e:
            print(f"Error resetting failures: {e}")
            return False


async def run_load_test(args):
    """运行负载测试"""
    # 创建负载生成器
    generator = LoadGenerator(
        target_url=args.target_url,
        kafka_servers=args.kafka_servers,
        kafka_topic=args.kafka_topic,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_channel=args.redis_channel,
        output_dir=args.output_dir
    )
    
    try:
        # 初始化连接
        await generator.initialize()
        
        # 运行负载测试
        metrics = await generator.run_load_test(
            mode=args.mode,
            duration=args.duration,
            rate=args.rate,
            ramp_up=args.ramp_up,
            data_file=args.data_file
        )
        
        # 打印结果摘要
        print("\nTest Results Summary:")
        print(f"  Throughput: {metrics.throughput:.2f} requests/second")
        print(f"  Average Latency: {metrics.avg_latency:.2f} ms")
        print(f"  P95 Latency: {metrics.p95_latency:.2f} ms")
        print(f"  P99 Latency: {metrics.p99_latency:.2f} ms")
        print(f"  Error Rate: {metrics.error_rate:.2f}%")
    finally:
        # 关闭连接
        await generator.close()


async def analyze_results(args):
    """分析测试结果"""
    # 创建性能分析器
    analyzer = PerformanceAnalyzer(results_dir=args.results_dir)
    
    # 获取测试文件
    test_files = []
    for file in os.listdir(args.results_dir):
        if file.endswith("_metrics.csv"):
            test_files.append(os.path.join(args.results_dir, file))
    
    if not test_files:
        print("No test result files found")
        return
    
    # 分析测试结果
    test_results = analyzer.analyze_test_results(test_files)
    
    # 比较测试结果
    comparison = analyzer.compare_tests(test_results)
    
    # 生成比较报告
    output_file = os.path.join(args.results_dir, "comparison_report.txt")
    analyzer.generate_comparison_report(comparison, output_file)
    
    # 生成比较图表
    output_prefix = os.path.join(args.results_dir, "comparison")
    analyzer.generate_comparison_charts(test_results, output_prefix)
    
    print(f"Analysis completed. Report saved to {output_file}")


async def inject_failure(args):
    """注入故障"""
    # 创建故障注入器
    injector = FailureInjector(target_url=args.target_url)
    
    try:
        # 初始化连接
        await injector.initialize()
        
        # 注入故障
        if args.failure_type == "node":
            success = await injector.inject_node_failure(args.node_id)
            if success:
                print(f"Node failure injected for node {args.node_id}")
            else:
                print("Failed to inject node failure")
        
        elif args.failure_type == "partition":
            node_ids = args.node_ids.split(",")
            success = await injector.inject_network_partition(node_ids)
            if success:
                print(f"Network partition injected for nodes {node_ids}")
            else:
                print("Failed to inject network partition")
        
        elif args.failure_type == "load":
            success = await injector.inject_high_load(args.duration)
            if success:
                print(f"High load injected for {args.duration} seconds")
            else:
                print("Failed to inject high load")
        
        elif args.failure_type == "latency":
            success = await injector.inject_slow_response(args.latency, args.duration)
            if success:
                print(f"Slow response injected with {args.latency}ms latency for {args.duration} seconds")
            else:
                print("Failed to inject slow response")
        
        elif args.failure_type == "reset":
            success = await injector.reset_failures()
            if success:
                print("All failures reset")
            else:
                print("Failed to reset failures")
    finally:
        # 关闭连接
        await injector.close()


async def main():
    """主函数"""
    # 创建主解析器
    parser = argparse.ArgumentParser(description="Performance Testing Tool")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # 负载测试子命令
    load_parser = subparsers.add_parser("load", help="Run load test")
    load_parser.add_argument("--mode", type=str, default="http", choices=["http", "kafka", "redis"], help="Test mode")
    load_parser.add_argument("--duration", type=int, default=60, help="Test duration in seconds")
    load_parser.add_argument("--rate", type=int, default=100, help="Target request rate per second")
    load_parser.add_argument("--ramp-up", type=int, default=10, help="Ramp-up time in seconds")
    load_parser.add_argument("--data-file", type=str, help="Data file path")
    load_parser.add_argument("--target-url", type=str, default="http://localhost:8000", help="Target URL")
    load_parser.add_argument("--kafka-servers", type=str, default="localhost:9092", help="Kafka bootstrap servers")
    load_parser.add_argument("--kafka-topic", type=str, default="vehicle-positions", help="Kafka topic")
    load_parser.add_argument("--redis-host", type=str, default="localhost", help="Redis host")
    load_parser.add_argument("--redis-port", type=int, default=6379, help="Redis port")
    load_parser.add_argument("--redis-channel", type=str, default="vehicle-positions", help="Redis channel")
    load_parser.add_argument("--output-dir", type=str, default="./results", help="Output directory")
    
    # 分析子命令
    analyze_parser = subparsers.add_parser("analyze", help="Analyze test results")
    analyze_parser.add_argument("--results-dir", type=str, default="./results", help="Results directory")
    
    # 故障注入子命令
    failure_parser = subparsers.add_parser("failure", help="Inject failure")
    failure_parser.add_argument("--target-url", type=str, default="http://localhost:8000", help="Target URL")
    failure_parser.add_argument("--failure-type", type=str, required=True, choices=["node", "partition", "load", "latency", "reset"], help="Failure type")
    failure_parser.add_argument("--node-id", type=str, help="Node ID for node failure")
    failure_parser.add_argument("--node-ids", type=str, help="Comma-separated node IDs for network partition")
    failure_parser.add_argument("--duration", type=int, default=60, help="Duration in seconds")
    failure_parser.add_argument("--latency", type=int, default=500, help="Latency in milliseconds")
    
    # 解析参数
    args = parser.parse_args()
    
    # 执行命令
    if args.command == "load":
        await run_load_test(args)
    elif args.command == "analyze":
        await analyze_results(args)
    elif args.command == "failure":
        await inject_failure(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    asyncio.run(main())
