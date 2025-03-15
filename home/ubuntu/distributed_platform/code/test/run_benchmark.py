#!/usr/bin/env python3
"""
基准测试脚本，用于测试分布式实时计算平台的性能
"""
import os
import sys
import time
import argparse
import subprocess
import asyncio
import signal
import json
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# 导入测试模块
from code.test.vehicle_simulator import TrafficMap, VehicleSimulator
from code.test.load_generator import LoadGenerator, PerformanceAnalyzer
from code.test.performance_monitor import PerformanceMonitor


async def run_benchmark(args):
    """运行基准测试"""
    print(f"开始基准测试，持续时间: {args.duration}秒，车辆数量: {args.vehicles}，目标TPS: {args.tps}")
    
    # 创建结果目录
    os.makedirs(args.results_dir, exist_ok=True)
    os.makedirs(args.monitoring_dir, exist_ok=True)
    os.makedirs(args.logs_dir, exist_ok=True)
    
    # 启动服务进程
    print("启动服务...")
    service_cmd = [
        "python3", "-m", "code.collision_system",
        f"--node-id={args.node_id}",
        f"--broker-url={args.kafka_servers}",
        f"--storage-url=redis://{args.redis_host}:{args.redis_port}",
        f"--api-port={args.api_port}",
        "--log-level=INFO"
    ]
    
    service_log = open(os.path.join(args.logs_dir, f"{args.node_id}.log"), "w")
    service_process = subprocess.Popen(
        service_cmd,
        stdout=service_log,
        stderr=service_log
    )
    
    # 等待服务启动
    print("等待服务启动...")
    await asyncio.sleep(5)
    
    # 启动性能监控
    print("启动性能监控...")
    monitor = PerformanceMonitor(
        api_url=f"http://localhost:{args.api_port}",
        output_dir=args.monitoring_dir,
        interval=1.0
    )
    await monitor.start()
    
    # 启动车辆模拟器
    print("启动车辆模拟器...")
    # 创建交通地图
    traffic_map = TrafficMap()
    traffic_map.generate_grid_map(10, 10)
    
    # 创建车辆模拟器
    simulator = VehicleSimulator(traffic_map, args.vehicles)
    simulator.initialize_vehicles("city_centered")
    
    # 创建负载生成器
    generator = LoadGenerator(
        target_url=f"http://localhost:{args.api_port}",
        kafka_servers=args.kafka_servers,
        kafka_topic="vehicle-positions",
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_channel="vehicle-positions",
        output_dir=args.results_dir
    )
    
    # 初始化连接
    await generator.initialize()
    
    # 模拟车辆更新
    print(f"开始模拟{args.vehicles}辆车的位置更新...")
    vehicle_task = asyncio.create_task(simulate_vehicles(simulator, args.duration, args.update_rate))
    
    # 等待一段时间，让车辆数据开始流动
    await asyncio.sleep(5)
    
    # 运行负载测试
    print(f"开始负载测试，目标TPS: {args.tps}...")
    metrics = await generator.run_load_test(
        mode="http",
        duration=args.duration - 10,  # 留出一些时间给其他操作
        rate=args.tps,
        ramp_up=30
    )
    
    # 等待车辆模拟完成
    await vehicle_task
    
    # 关闭连接
    await generator.close()
    
    # 停止性能监控
    await monitor.stop()
    
    # 停止服务
    print("停止服务...")
    service_process.send_signal(signal.SIGTERM)
    service_process.wait()
    service_log.close()
    
    # 分析结果
    print("分析测试结果...")
    analyzer = PerformanceAnalyzer(results_dir=args.results_dir)
    
    # 获取测试文件
    test_files = []
    for file in os.listdir(args.results_dir):
        if file.endswith("_metrics.csv"):
            test_files.append(os.path.join(args.results_dir, file))
    
    if test_files:
        # 分析测试结果
        test_results = analyzer.analyze_test_results(test_files)
        
        # 生成比较报告
        if test_results:
            comparison = analyzer.compare_tests(test_results)
            output_file = os.path.join(args.results_dir, "benchmark_report.txt")
            analyzer.generate_comparison_report(comparison, output_file)
            
            # 生成比较图表
            output_prefix = os.path.join(args.results_dir, "benchmark")
            analyzer.generate_comparison_charts(test_results, output_prefix)
            
            print(f"分析完成。报告保存到 {output_file}")
    else:
        print("没有找到测试结果文件")
    
    # 保存测试摘要
    summary = {
        "timestamp": datetime.now().isoformat(),
        "duration": args.duration,
        "vehicles": args.vehicles,
        "target_tps": args.tps,
        "throughput": metrics.throughput,
        "avg_latency": metrics.avg_latency,
        "p95_latency": metrics.p95_latency,
        "p99_latency": metrics.p99_latency,
        "error_rate": metrics.error_rate,
        "cpu_usage": metrics.cpu_usage,
        "memory_usage": metrics.memory_usage
    }
    
    with open(os.path.join(args.results_dir, "benchmark_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    
    print("\n基准测试完成。结果摘要:")
    print(f"  吞吐量: {metrics.throughput:.2f} 请求/秒")
    print(f"  平均延迟: {metrics.avg_latency:.2f} ms")
    print(f"  P95延迟: {metrics.p95_latency:.2f} ms")
    print(f"  P99延迟: {metrics.p99_latency:.2f} ms")
    print(f"  错误率: {metrics.error_rate:.2f}%")
    print(f"  CPU使用率: {metrics.cpu_usage:.2f}%")
    print(f"  内存使用率: {metrics.memory_usage:.2f}%")
    
    return metrics


async def simulate_vehicles(simulator, duration, update_rate):
    """模拟车辆位置更新"""
    start_time = time.time()
    update_count = 0
    
    while time.time() - start_time < duration:
        # 更新车辆
        simulator.update_vehicles(update_rate)
        update_count += 1
        
        # 打印进度
        if update_count % 10 == 0:
            elapsed = time.time() - start_time
            print(f"车辆模拟进度: {elapsed:.1f}/{duration}秒, {update_count}次更新")
        
        # 等待下一次更新
        await asyncio.sleep(update_rate)


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="分布式实时计算平台基准测试")
    parser.add_argument("--duration", type=int, default=300, help="测试持续时间（秒）")
    parser.add_argument("--vehicles", type=int, default=10000, help="模拟车辆数量")
    parser.add_argument("--update-rate", type=float, default=1.0, help="车辆位置更新频率（秒）")
    parser.add_argument("--tps", type=int, default=10000, help="目标TPS")
    parser.add_argument("--node-id", type=str, default="node-1", help="节点ID")
    parser.add_argument("--api-port", type=int, default=8000, help="API端口")
    parser.add_argument("--kafka-servers", type=str, default="localhost:9092", help="Kafka服务器")
    parser.add_argument("--redis-host", type=str, default="localhost", help="Redis主机")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis端口")
    parser.add_argument("--results-dir", type=str, default="./results", help="结果目录")
    parser.add_argument("--monitoring-dir", type=str, default="./monitoring", help="监控目录")
    parser.add_argument("--logs-dir", type=str, default="./logs", help="日志目录")
    
    args = parser.parse_args()
    
    try:
        await run_benchmark(args)
    except KeyboardInterrupt:
        print("测试被用户中断")
    except Exception as e:
        print(f"测试过程中出错: {e}")


if __name__ == "__main__":
    asyncio.run(main())
