# 部署指南

## 1. 简介

本文档提供了分布式实时计算平台和3D碰撞检测系统的详细部署指南。按照本指南，您可以在单机环境或集群环境中部署和配置系统。

## 2. 系统要求

### 2.1 硬件要求

#### 最小配置（开发/测试环境）

- CPU: 4核心
- 内存: 16GB RAM
- 存储: 100GB SSD
- 网络: 1Gbps网络接口

#### 推荐配置（生产环境）

- CPU: 16+核心
- 内存: 64GB+ RAM
- 存储: 1TB+ SSD（RAID配置）
- 网络: 10Gbps网络接口

#### 集群配置（高吞吐量生产环境）

- 计算节点: 10+台服务器，每台16+核心，64GB+内存
- 存储节点: 3+台服务器，每台8+核心，32GB+内存，10TB+存储
- 管理节点: 3台服务器，每台8+核心，32GB+内存
- 网络: 10Gbps内部网络，冗余连接

### 2.2 软件要求

- 操作系统: Ubuntu 20.04 LTS或更高版本
- Python: 3.10或更高版本
- Docker: 20.10或更高版本
- Kubernetes: 1.22或更高版本（集群部署）
- 数据库: PostgreSQL 14或更高版本，TimescaleDB扩展
- 消息队列: Kafka 3.0或更高版本
- 内存数据库: Redis 6.0或更高版本
- 监控系统: Prometheus, Grafana

## 3. 单机部署

### 3.1 准备环境

1. 安装操作系统和基础软件：

```bash
# 更新系统
sudo apt update && sudo apt upgrade -y

# 安装基础工具
sudo apt install -y build-essential git curl wget unzip

# 安装Python
sudo apt install -y python3 python3-pip python3-dev

# 安装Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER
```

2. 安装数据库和消息队列：

```bash
# 安装PostgreSQL和TimescaleDB
sudo apt install -y postgresql postgresql-contrib
sudo add-apt-repository ppa:timescale/timescaledb-ppa
sudo apt update
sudo apt install -y timescaledb-postgresql-14

# 配置TimescaleDB
sudo timescaledb-tune --quiet --yes

# 安装Redis
sudo apt install -y redis-server

# 安装Kafka（使用Docker）
mkdir -p ~/kafka
cd ~/kafka
wget https://raw.githubusercontent.com/confluentinc/cp-all-in-one/7.3.0-post/cp-all-in-one/docker-compose.yml
docker-compose up -d
```

### 3.2 获取代码

```bash
# 克隆代码仓库
git clone https://github.com/collision-detection-platform/distributed-platform.git
cd distributed-platform

# 安装依赖
pip3 install -r requirements.txt
```

### 3.3 配置系统

1. 创建配置文件：

```bash
cp config/config.example.yaml config/config.yaml
```

2. 编辑配置文件，设置数据库连接、消息队列连接等：

```bash
nano config/config.yaml
```

配置文件示例：

```yaml
system:
  name: "distributed-platform"
  environment: "development"  # development, testing, production
  log_level: "info"  # debug, info, warning, error
  max_tps: 10000
  throttling_threshold: 0.9

database:
  postgres:
    host: "localhost"
    port: 5432
    database: "collision_detection"
    user: "postgres"
    password: "your_password"
  redis:
    host: "localhost"
    port: 6379
    db: 0
    password: ""

messaging:
  kafka:
    bootstrap_servers: "localhost:9092"
    topics:
      vehicle_locations: "vehicle-locations"
      collision_risks: "collision-risks"
      system_events: "system-events"

compute:
  max_workers: 8
  batch_size: 100
  processing_interval: 0.1

grid:
  initial_size: 100.0
  min_size: 10.0
  max_size: 1000.0
  adjustment_threshold: 0.8
  adjustment_factor: 0.5

collision_detection:
  prediction_time: 10.0
  min_risk_level: 0.3
  max_distance: 500.0

api:
  host: "0.0.0.0"
  port: 8080
  cors_origins: ["*"]
  rate_limit:
    enabled: true
    default_limit: "100/minute"
```

3. 创建数据库：

```bash
# 登录PostgreSQL
sudo -u postgres psql

# 创建数据库和用户
CREATE DATABASE collision_detection;
CREATE USER platform_user WITH ENCRYPTED PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE collision_detection TO platform_user;

# 退出PostgreSQL
\q

# 初始化数据库
cd distributed-platform
python3 scripts/init_database.py
```

### 3.4 启动系统

1. 启动各组件：

```bash
# 启动API服务
python3 api/main.py &

# 启动计算节点
python3 compute/node.py --node-id node-1 &

# 启动调度器
python3 scheduler/scheduler.py &

# 启动监控服务
python3 monitoring/monitor.py &
```

2. 或者使用提供的启动脚本：

```bash
./scripts/start_all.sh
```

3. 验证系统是否正常运行：

```bash
# 检查API服务
curl http://localhost:8080/system/status

# 检查日志
tail -f logs/api.log
tail -f logs/compute.log
tail -f logs/scheduler.log
```

### 3.5 停止系统

```bash
# 使用停止脚本
./scripts/stop_all.sh

# 或者手动停止
pkill -f "python3 api/main.py"
pkill -f "python3 compute/node.py"
pkill -f "python3 scheduler/scheduler.py"
pkill -f "python3 monitoring/monitor.py"
```

## 4. Docker部署

### 4.1 使用Docker Compose

1. 确保已安装Docker和Docker Compose：

```bash
# 安装Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.15.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

2. 使用提供的Docker Compose文件：

```bash
cd distributed-platform
docker-compose up -d
```

3. 验证容器是否正常运行：

```bash
docker-compose ps
```

4. 查看日志：

```bash
docker-compose logs -f
```

5. 停止系统：

```bash
docker-compose down
```

### 4.2 自定义Docker部署

1. 构建Docker镜像：

```bash
cd distributed-platform
docker build -t collision-detection-platform:latest .
```

2. 运行容器：

```bash
# 运行API服务
docker run -d --name api-service -p 8080:8080 -v $(pwd)/config:/app/config collision-detection-platform:latest python3 api/main.py

# 运行计算节点
docker run -d --name compute-node-1 -v $(pwd)/config:/app/config collision-detection-platform:latest python3 compute/node.py --node-id node-1

# 运行调度器
docker run -d --name scheduler -v $(pwd)/config:/app/config collision-detection-platform:latest python3 scheduler/scheduler.py

# 运行监控服务
docker run -d --name monitor -v $(pwd)/config:/app/config collision-detection-platform:latest python3 monitoring/monitor.py
```

## 5. Kubernetes部署

### 5.1 准备Kubernetes集群

1. 安装Minikube（本地开发/测试）：

```bash
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube
minikube start --driver=docker --cpus=4 --memory=8g
```

或者使用云服务提供商的Kubernetes服务（AWS EKS, Google GKE, Azure AKS等）。

2. 安装kubectl：

```bash
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
```

### 5.2 部署系统

1. 应用Kubernetes配置：

```bash
cd distributed-platform/kubernetes
kubectl apply -f namespace.yaml
kubectl apply -f configmap.yaml
kubectl apply -f secret.yaml
kubectl apply -f postgres.yaml
kubectl apply -f redis.yaml
kubectl apply -f kafka.yaml
kubectl apply -f api-service.yaml
kubectl apply -f compute-nodes.yaml
kubectl apply -f scheduler.yaml
kubectl apply -f monitoring.yaml
```

2. 或者使用Helm Chart：

```bash
cd distributed-platform/helm
helm install collision-detection-platform ./collision-detection-platform
```

3. 验证部署：

```bash
kubectl get pods -n collision-detection
```

4. 查看日志：

```bash
kubectl logs -f deployment/api-service -n collision-detection
kubectl logs -f deployment/compute-nodes -n collision-detection
```

5. 暴露API服务：

```bash
kubectl port-forward service/api-service 8080:8080 -n collision-detection
```

### 5.3 扩展系统

1. 扩展计算节点：

```bash
kubectl scale deployment compute-nodes --replicas=5 -n collision-detection
```

2. 自动扩展（需要配置Horizontal Pod Autoscaler）：

```bash
kubectl apply -f hpa.yaml
```

## 6. 生产环境配置

### 6.1 安全配置

1. 配置HTTPS：

```bash
# 生成自签名证书（仅用于测试）
openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout server.key -out server.crt

# 在配置文件中启用HTTPS
nano config/config.yaml
```

```yaml
api:
  host: "0.0.0.0"
  port: 8443
  ssl:
    enabled: true
    cert_file: "server.crt"
    key_file: "server.key"
```

2. 配置认证：

```yaml
api:
  auth:
    enabled: true
    jwt_secret: "your_jwt_secret"
    token_expiry: 86400  # 24小时
```

3. 配置防火墙：

```bash
# 允许API端口
sudo ufw allow 8443/tcp

# 允许内部通信端口
sudo ufw allow from 10.0.0.0/8 to any port 9092  # Kafka
sudo ufw allow from 10.0.0.0/8 to any port 6379  # Redis
sudo ufw allow from 10.0.0.0/8 to any port 5432  # PostgreSQL
```

### 6.2 高可用性配置

1. 配置数据库复制：

```bash
# 在PostgreSQL主节点上
sudo -u postgres psql -c "ALTER SYSTEM SET wal_level = replica;"
sudo -u postgres psql -c "ALTER SYSTEM SET max_wal_senders = 10;"
sudo -u postgres psql -c "ALTER SYSTEM SET max_replication_slots = 10;"
sudo systemctl restart postgresql

# 配置从节点
# ...
```

2. 配置Redis哨兵：

```bash
# 创建哨兵配置文件
cat > /etc/redis/sentinel.conf << EOF
sentinel monitor mymaster 127.0.0.1 6379 2
sentinel down-after-milliseconds mymaster 5000
sentinel failover-timeout mymaster 60000
EOF

# 启动哨兵
redis-sentinel /etc/redis/sentinel.conf
```

3. 配置Kafka集群：

```bash
# 修改Kafka配置
nano /etc/kafka/server.properties
```

```properties
broker.id=0
zookeeper.connect=zk1:2181,zk2:2181,zk3:2181
replica.factor=3
min.insync.replicas=2
```

### 6.3 监控配置

1. 安装Prometheus和Grafana：

```bash
# 安装Prometheus
wget https://github.com/prometheus/prometheus/releases/download/v2.37.0/prometheus-2.37.0.linux-amd64.tar.gz
tar xvfz prometheus-2.37.0.linux-amd64.tar.gz
cd prometheus-2.37.0.linux-amd64
./prometheus --config.file=prometheus.yml

# 安装Grafana
sudo apt-get install -y apt-transport-https software-properties-common
wget -q -O - https://packages.grafana.com/gpg.key | sudo apt-key add -
sudo add-apt-repository "deb https://packages.grafana.com/oss/deb stable main"
sudo apt-get update
sudo apt-get install grafana
sudo systemctl start grafana-server
```

2. 配置Prometheus抓取目标：

```yaml
scrape_configs:
  - job_name: 'collision-detection-platform'
    scrape_interval: 15s
    static_configs:
      - targets: ['localhost:8080']
```

3. 导入Grafana仪表板：

访问`http://localhost:3000`，导入提供的Grafana仪表板JSON文件。

### 6.4 备份配置

1. 配置数据库备份：

```bash
# 创建备份脚本
cat > backup_db.sh << EOF
#!/bin/bash
TIMESTAMP=\$(date +%Y%m%d_%H%M%S)
BACKUP_DIR=/var/backups/postgres
mkdir -p \$BACKUP_DIR
pg_dump -U postgres collision_detection | gzip > \$BACKUP_DIR/collision_detection_\$TIMESTAMP.sql.gz
find \$BACKUP_DIR -type f -mtime +7 -delete
EOF

chmod +x backup_db.sh

# 添加到crontab
(crontab -l 2>/dev/null; echo "0 2 * * * /path/to/backup_db.sh") | crontab -
```

2. 配置系统状态备份：

```bash
# 创建状态备份脚本
cat > backup_state.sh << EOF
#!/bin/bash
TIMESTAMP=\$(date +%Y%m%d_%H%M%S)
BACKUP_DIR=/var/backups/platform
mkdir -p \$BACKUP_DIR
curl -s http://localhost:8080/system/export-state > \$BACKUP_DIR/state_\$TIMESTAMP.json
find \$BACKUP_DIR -type f -mtime +7 -delete
EOF

chmod +x backup_state.sh

# 添加到crontab
(crontab -l 2>/dev/null; echo "0 */6 * * * /path/to/backup_state.sh") | crontab -
```

## 7. 集群部署

### 7.1 集群架构

推荐的集群架构：

1. 负载均衡层：
   - 2+ 负载均衡器（如NGINX, HAProxy）
   - 配置健康检查和故障转移

2. API层：
   - 3+ API服务节点
   - 无状态设计，便于水平扩展

3. 计算层：
   - 10+ 计算节点
   - 按地理区域或负载分布

4. 数据层：
   - PostgreSQL主从复制（1主2从）
   - Redis集群（3主3从）
   - Kafka集群（3+ broker）

5. 监控层：
   - Prometheus + Grafana
   - 日志聚合（ELK Stack）

### 7.2 网络配置

1. 配置内部网络：

```bash
# 创建内部网络
docker network create --subnet=172.20.0.0/16 platform-network

# 或在Kubernetes中
kubectl apply -f network-policy.yaml
```

2. 配置负载均衡：

```bash
# NGINX配置示例
cat > /etc/nginx/conf.d/platform.conf << EOF
upstream api_servers {
    server api1:8080;
    server api2:8080;
    server api3:8080;
}

server {
    listen 80;
    server_name api.collision-detection-platform.com;

    location / {
        proxy_pass http://api_servers;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
    }
}
EOF

# 重启NGINX
sudo systemctl restart nginx
```

### 7.3 服务发现

1. 使用ZooKeeper或etcd进行服务发现：

```bash
# 安装ZooKeeper
wget https://dlcdn.apache.org/zookeeper/zookeeper-3.7.1/apache-zookeeper-3.7.1-bin.tar.gz
tar -xzf apache-zookeeper-3.7.1-bin.tar.gz
cd apache-zookeeper-3.7.1-bin
cp conf/zoo_sample.cfg conf/zoo.cfg
./bin/zkServer.sh start
```

2. 在配置文件中启用服务发现：

```yaml
discovery:
  enabled: true
  provider: "zookeeper"
  connection: "localhost:2181"
  base_path: "/collision-detection-platform"
  register_interval: 30
```

### 7.4 集群管理

1. 使用Kubernetes进行集群管理：

```bash
# 部署管理工具
kubectl apply -f kubernetes/management/

# 查看集群状态
kubectl get pods -n collision-detection
kubectl top nodes
```

2. 使用自定义管理脚本：

```bash
# 集群状态检查
./scripts/cluster_status.sh

# 添加新节点
./scripts/add_node.sh --type compute --id node-11

# 移除节点
./scripts/remove_node.sh --id node-5
```

## 8. 升级和维护

### 8.1 升级流程

1. 准备升级：

```bash
# 备份数据
./scripts/backup_all.sh

# 下载新版本
git fetch
git checkout v1.2.0
```

2. 执行升级：

```bash
# 停止服务
./scripts/stop_all.sh

# 更新数据库架构
python3 scripts/migrate_database.py

# 启动新版本
./scripts/start_all.sh
```

3. 验证升级：

```bash
# 检查版本
curl http://localhost:8080/system/version

# 运行测试
python3 -m pytest tests/
```

### 8.2 回滚流程

如果升级失败，执行回滚：

```bash
# 停止服务
./scripts/stop_all.sh

# 切换回旧版本
git checkout v1.1.0

# 恢复数据库
./scripts/restore_database.sh --backup latest

# 启动旧版本
./scripts/start_all.sh
```

### 8.3 日常维护

1. 日志轮转：

```bash
# 配置logrotate
cat > /etc/logrotate.d/collision-detection << EOF
/var/log/collision-detection/*.log {
    daily
    missingok
    rotate 14
    compress
    delaycompress
    notifempty
    create 0640 ubuntu ubuntu
    sharedscripts
    postrotate
        systemctl reload collision-detection-platform.service
    endscript
}
EOF
```

2. 数据清理：

```bash
# 创建数据清理脚本
cat > clean_old_data.sh << EOF
#!/bin/bash
# 清理30天前的历史数据
psql -U postgres -d collision_detection -c "DELETE FROM vehicle_locations WHERE timestamp < NOW() - INTERVAL '30 days';"
psql -U postgres -d collision_detection -c "DELETE FROM collision_risks WHERE timestamp < NOW() - INTERVAL '30 days';"
EOF

chmod +x clean_old_data.sh

# 添加到crontab
(crontab -l 2>/dev/null; echo "0 3 * * * /path/to/clean_old_data.sh") | crontab -
```

## 9. 故障排除

### 9.1 常见问题

1. 服务无法启动：

```bash
# 检查日志
tail -f logs/api.log

# 检查配置
python3 scripts/validate_config.py

# 检查端口占用
netstat -tulpn | grep 8080
```

2. 数据库连接问题：

```bash
# 检查PostgreSQL状态
sudo systemctl status postgresql

# 检查连接
psql -U postgres -d collision_detection -c "SELECT 1;"

# 检查日志
tail -f /var/log/postgresql/postgresql-14-main.log
```

3. 性能问题：

```bash
# 检查系统资源
top
iostat -x 1
free -m

# 检查数据库性能
psql -U postgres -d collision_detection -c "SELECT * FROM pg_stat_activity;"

# 检查网络
iftop -i eth0
```

### 9.2 日志分析

1. 收集日志：

```bash
# 收集所有日志
./scripts/collect_logs.sh

# 分析错误
grep ERROR logs/*.log

# 分析性能问题
grep "slow query" logs/*.log
```

2. 使用日志分析工具：

```bash
# 安装ELK Stack
# ...

# 发送日志到Elasticsearch
filebeat -e -c filebeat.yml
```

### 9.3 联系支持

如果您遇到无法解决的问题，请联系技术支持：

- 邮件：support@collision-detection-platform.com
- 电话：+1-234-567-8900
- 在线支持：https://support.collision-detection-platform.com

请提供以下信息以便我们更好地帮助您：

- 系统版本
- 部署环境
- 错误日志
- 复现步骤
- 系统状态报告（`./scripts/system_report.sh`）

## 10. 附录

### 10.1 配置参数参考

| 参数 | 描述 | 默认值 | 推荐值（生产环境） |
|------|------|--------|-------------------|
| system.max_tps | 系统最大TPS | 10000 | 根据硬件配置调整 |
| compute.max_workers | 每个计算节点的工作线程数 | 8 | CPU核心数 |
| grid.initial_size | 初始网格大小 | 100.0 | 100.0 |
| collision_detection.prediction_time | 碰撞预测时间（秒） | 10.0 | 5.0-15.0 |

### 10.2 命令参考

| 命令 | 描述 |
|------|------|
| ./scripts/start_all.sh | 启动所有服务 |
| ./scripts/stop_all.sh | 停止所有服务 |
| ./scripts/status.sh | 查看服务状态 |
| ./scripts/backup_all.sh | 备份所有数据 |
| ./scripts/restore_database.sh | 恢复数据库 |
| ./scripts/add_node.sh | 添加新节点 |
| ./scripts/remove_node.sh | 移除节点 |
| ./scripts/collect_logs.sh | 收集日志 |
| ./scripts/system_report.sh | 生成系统报告 |

### 10.3 文件和目录结构

```
distributed-platform/
├── api/                  # API服务
├── compute/              # 计算节点
├── scheduler/            # 调度器
├── common/               # 共享代码
├── collision/            # 碰撞检测算法
├── reliability/          # 可靠性功能
├── monitoring/           # 监控系统
├── storage/              # 存储接口
├── config/               # 配置文件
├── scripts/              # 脚本工具
├── tests/                # 测试代码
├── docs/                 # 文档
├── kubernetes/           # Kubernetes配置
├── helm/                 # Helm Chart
├── docker-compose.yml    # Docker Compose配置
├── Dockerfile            # Docker镜像构建
└── requirements.txt      # Python依赖
```

### 10.4 性能调优建议

1. 数据库调优：

```bash
# 调整PostgreSQL配置
sudo nano /etc/postgresql/14/main/postgresql.conf
```

```
shared_buffers = 4GB
work_mem = 64MB
maintenance_work_mem = 512MB
effective_cache_size = 12GB
max_connections = 200
```

2. 系统调优：

```bash
# 调整系统参数
sudo nano /etc/sysctl.conf
```

```
fs.file-max = 2097152
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
net.ipv4.tcp_fin_timeout = 30
net.ipv4.tcp_keepalive_time = 300
vm.swappiness = 10
```

3. 应用调优：

```yaml
# 调整应用配置
compute:
  max_workers: 16
  batch_size: 200
  processing_interval: 0.05

grid:
  adjustment_threshold: 0.7
  adjustment_factor: 0.6
```

### 10.5 安全最佳实践

1. 定期更新系统：

```bash
# 更新系统
sudo apt update && sudo apt upgrade -y

# 更新应用
git pull
pip3 install -r requirements.txt
```

2. 加密敏感数据：

```yaml
# 配置数据加密
security:
  encryption:
    enabled: true
    key_file: "/etc/collision-detection/encryption.key"
```

3. 实施访问控制：

```bash
# 创建专用用户
sudo adduser --system --group collision-detection
sudo chown -R collision-detection:collision-detection /opt/collision-detection

# 限制文件权限
sudo chmod 600 /etc/collision-detection/config.yaml
```

4. 启用审计日志：

```yaml
# 配置审计日志
logging:
  audit:
    enabled: true
    file: "/var/log/collision-detection/audit.log"
    events:
      - "login"
      - "config_change"
      - "api_access"
```

### 10.6 灾难恢复计划

1. 准备灾难恢复计划文档，包括：
   - 备份策略和恢复程序
   - 关键系统和数据的优先级
   - 恢复时间目标（RTO）和恢复点目标（RPO）
   - 联系人和责任分配
   - 测试计划和时间表

2. 定期测试灾难恢复流程：

```bash
# 模拟故障
./scripts/simulate_failure.sh --component database

# 执行恢复
./scripts/disaster_recovery.sh --component database
```

3. 记录和改进恢复流程：
   - 记录每次测试的结果和问题
   - 更新恢复程序以解决发现的问题
   - 定期审查和更新灾难恢复计划
