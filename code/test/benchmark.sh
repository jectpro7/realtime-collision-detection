#!/bin/bash
# 基准测试脚本，用于测试分布式实时计算平台的性能

# 创建结果目录
mkdir -p /home/ubuntu/distributed_platform/results
mkdir -p /home/ubuntu/distributed_platform/monitoring

# 设置测试参数
TEST_DURATION=300  # 测试持续时间（秒）
VEHICLE_COUNT=10000  # 模拟车辆数量
UPDATE_RATE=1.0  # 车辆位置更新频率（秒）
TARGET_TPS=10000  # 目标TPS
API_URL="http://localhost:8000"  # API URL
KAFKA_SERVERS="localhost:9092"  # Kafka服务器
REDIS_HOST="localhost"  # Redis主机
REDIS_PORT=6379  # Redis端口

# 启动服务
echo "启动服务..."
cd /home/ubuntu/distributed_platform
python3 -m code.collision_system --node-id=node-1 --broker-url=$KAFKA_SERVERS --storage-url="redis://$REDIS_HOST:$REDIS_PORT" --api-port=8000 --log-level=INFO > ./logs/node-1.log 2>&1 &
NODE1_PID=$!
sleep 5  # 等待服务启动

# 启动性能监控
echo "启动性能监控..."
python3 -m code.test.performance_monitor --api-url=$API_URL --output-dir=./monitoring --interval=1.0 > ./logs/monitor.log 2>&1 &
MONITOR_PID=$!

# 启动车辆模拟器
echo "启动车辆模拟器..."
python3 -m code.test.vehicle_simulator --num-vehicles=$VEHICLE_COUNT --update-rate=$UPDATE_RATE --distribution=city_centered --output=kafka --kafka-servers=$KAFKA_SERVERS --kafka-topic=vehicle-positions --run-time=$TEST_DURATION > ./logs/simulator.log 2>&1 &
SIMULATOR_PID=$!

# 等待车辆模拟器启动
sleep 10

# 运行负载测试
echo "运行负载测试..."
python3 -m code.test.load_generator load --mode=http --duration=$TEST_DURATION --rate=$TARGET_TPS --ramp-up=30 --target-url=$API_URL --output-dir=./results > ./logs/load_test.log 2>&1

# 等待所有测试完成
echo "等待测试完成..."
wait $SIMULATOR_PID

# 分析结果
echo "分析测试结果..."
python3 -m code.test.load_generator analyze --results-dir=./results > ./results/analysis.log 2>&1

# 停止服务和监控
echo "停止服务和监控..."
kill $MONITOR_PID
kill $NODE1_PID

echo "基准测试完成。结果保存在 ./results 和 ./monitoring 目录中。"
