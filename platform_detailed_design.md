# 分布式计算平台详细设计

## 1. 数据流架构设计

### 1.1 数据流概述

数据流是整个系统的核心，需要确保高吞吐、低延迟和可靠性。整体数据流路径如下：

```
车辆位置数据 → 接入网关 → 消息队列 → 空间分区服务 → 计算节点 → 结果聚合 → 存储/预警
```

### 1.2 数据接入层设计

#### 1.2.1 接入网关

接入网关负责接收来自车辆的位置数据，并进行初步验证和预处理。

**组件设计**：
- **协议支持**：支持MQTT、HTTP和WebSocket协议
- **认证授权**：JWT或API密钥认证
- **限流保护**：基于令牌桶算法的限流机制
- **数据验证**：验证数据格式和完整性
- **负载均衡**：使用一致性哈希算法，确保相同车辆的数据路由到相同的处理节点

**接口设计**：
```python
# 位置数据接口
class LocationData:
    vehicle_id: str       # 车辆唯一标识
    timestamp: float      # 时间戳(秒)
    position: Position    # 3D位置
    velocity: Vector      # 速度向量
    heading: float        # 朝向角度
    vehicle_type: str     # 车辆类型
    
# 位置数据接收接口
async def receive_location(location_data: LocationData) -> bool:
    # 验证数据
    # 预处理
    # 发送到消息队列
    pass
```

### 1.3 消息传输层设计

#### 1.3.1 消息队列

消息队列负责缓冲和传输数据，确保数据不丢失，并支持高吞吐量处理。

**技术选择**：Apache Kafka 或 Apache Pulsar

**分区策略**：
- 基于地理位置的分区策略
- 将3D空间划分为网格，每个网格对应一个分区
- 确保相邻网格的数据在相邻分区，便于边界处理

**主题设计**：
- `vehicle-locations`：车辆位置数据主题
- `grid-partitioned-data`：经过空间分区后的数据主题
- `collision-alerts`：碰撞预警主题
- `system-metrics`：系统指标主题

**消息格式**：
```python
# Kafka消息格式
class KafkaMessage:
    key: bytes            # 分区键(网格ID)
    value: bytes          # 序列化的位置数据
    timestamp: int        # 消息时间戳
    headers: Dict[str, bytes]  # 元数据
```

### 1.4 空间分区服务设计

空间分区服务负责将3D空间划分为网格，并将车辆位置数据分配到相应的网格中。

**分区算法**：
- **自适应网格**：根据车辆密度动态调整网格大小
- **四叉树/八叉树**：用于空间划分的数据结构
- **负载均衡**：确保每个计算节点的负载相对均衡

**实现策略**：
```python
class SpatialPartitioner:
    def __init__(self, initial_grid_size: float, min_grid_size: float, max_grid_size: float):
        self.grid_size = initial_grid_size
        self.min_grid_size = min_grid_size
        self.max_grid_size = max_grid_size
        self.grid_density = {}  # 网格密度统计
        
    def get_grid_id(self, position: Position) -> str:
        # 计算位置所属的网格ID
        pass
        
    def adjust_grid_size(self):
        # 根据密度统计动态调整网格大小
        pass
        
    def process_location_data(self, location_data: LocationData) -> Tuple[str, LocationData]:
        # 处理位置数据，返回(网格ID, 位置数据)
        grid_id = self.get_grid_id(location_data.position)
        self.update_density(grid_id)
        return (grid_id, location_data)
```

## 2. 计算节点架构设计

### 2.1 计算节点概述

计算节点是执行碰撞检测算法的核心组件，需要高效处理大量空间数据。

### 2.2 计算节点内部架构

**组件设计**：
- **数据接收器**：从消息队列接收数据
- **空间索引**：维护网格内车辆的空间索引
- **碰撞检测器**：执行碰撞检测算法
- **结果发送器**：将检测结果发送到结果聚合服务

**状态管理**：
- **内存状态**：维护最新的车辆位置和轨迹
- **持久化策略**：定期将状态快照持久化，支持故障恢复
- **状态同步**：节点间的状态同步机制，处理车辆跨网格移动

**并行处理**：
- **多线程模型**：使用线程池处理并行计算
- **任务分解**：将碰撞检测任务分解为可并行的子任务
- **负载均衡**：动态调整线程分配，确保CPU资源充分利用

**实现框架**：
```python
class ComputeNode:
    def __init__(self, grid_id: str, config: NodeConfig):
        self.grid_id = grid_id
        self.config = config
        self.spatial_index = SpatialIndex()
        self.vehicle_states = {}  # 车辆状态缓存
        self.thread_pool = ThreadPoolExecutor(max_workers=config.max_workers)
        
    async def process_batch(self, location_data_batch: List[LocationData]):
        # 批量处理位置数据
        tasks = []
        for data in location_data_batch:
            tasks.append(self.process_single(data))
        return await asyncio.gather(*tasks)
        
    async def process_single(self, data: LocationData):
        # 处理单条位置数据
        self.update_vehicle_state(data)
        nearby_vehicles = self.spatial_index.query_nearby(data.position, self.config.search_radius)
        collision_risks = self.detect_collisions(data, nearby_vehicles)
        if collision_risks:
            await self.send_alerts(collision_risks)
        
    def update_vehicle_state(self, data: LocationData):
        # 更新车辆状态和空间索引
        pass
        
    def detect_collisions(self, data: LocationData, nearby_vehicles: List[VehicleState]) -> List[CollisionRisk]:
        # 执行碰撞检测算法
        pass
        
    async def send_alerts(self, collision_risks: List[CollisionRisk]):
        # 发送碰撞风险警报
        pass
```

## 3. 调度系统设计

### 3.1 调度系统概述

调度系统负责管理计算资源，分配任务，确保系统高效运行。

### 3.2 调度器设计

**调度策略**：
- **网格感知调度**：根据网格的车辆密度分配计算资源
- **负载均衡**：确保计算节点负载均衡
- **资源弹性**：根据负载动态扩缩容计算节点
- **任务优先级**：支持任务优先级，确保关键任务优先处理

**调度器组件**：
- **资源管理器**：管理计算节点资源
- **任务分发器**：将任务分发到合适的计算节点
- **负载监控器**：监控计算节点的负载情况
- **扩缩容控制器**：控制计算节点的扩缩容

**实现框架**：
```python
class Scheduler:
    def __init__(self, config: SchedulerConfig):
        self.config = config
        self.nodes = {}  # 计算节点注册表
        self.grid_load = {}  # 网格负载统计
        
    def register_node(self, node_id: str, node_info: NodeInfo):
        # 注册计算节点
        pass
        
    def unregister_node(self, node_id: str):
        # 注销计算节点
        pass
        
    def update_grid_load(self, grid_id: str, load_metrics: LoadMetrics):
        # 更新网格负载统计
        pass
        
    def schedule_task(self, task: Task) -> str:
        # 调度任务到合适的计算节点，返回节点ID
        pass
        
    def balance_load(self):
        # 执行负载均衡
        pass
        
    def scale_nodes(self):
        # 根据负载情况扩缩容计算节点
        pass
```

### 3.3 服务发现机制

**服务注册**：
- 使用etcd作为服务注册中心
- 计算节点启动时注册自身信息
- 定期发送心跳保持注册有效

**服务发现**：
- 调度器从etcd获取可用计算节点列表
- 支持基于标签的服务过滤
- 缓存服务信息，减少查询开销

## 4. 数据存储方案设计

### 4.1 存储需求分析

**数据类型**：
- **实时位置数据**：车辆当前位置和状态
- **历史轨迹数据**：车辆历史位置记录
- **碰撞风险数据**：检测到的碰撞风险记录
- **系统元数据**：网格配置、节点信息等

**存储特性需求**：
- **实时数据**：低延迟读写，高并发访问
- **历史数据**：高效的时间范围查询，支持大数据量
- **空间数据**：支持空间索引和地理查询
- **元数据**：强一致性，支持事务

### 4.2 存储架构设计

**多层存储架构**：
- **内存层**：Redis/Aerospike存储实时位置数据
- **时序层**：TimescaleDB存储历史轨迹数据
- **关系层**：PostgreSQL+PostGIS存储元数据和空间关系
- **对象存储层**：MinIO存储大容量历史数据归档

**数据分片策略**：
- **时间分片**：按时间范围分片历史数据
- **空间分片**：按地理区域分片位置数据
- **ID分片**：按车辆ID范围分片车辆数据

**缓存策略**：
- **多级缓存**：L1(节点内存)→L2(Redis)→L3(数据库)
- **缓存预热**：系统启动时预加载热点数据
- **缓存一致性**：基于版本号的缓存一致性机制

### 4.3 数据模型设计

**实时位置数据模型**：
```python
# Redis数据模型
class VehiclePosition:
    key: str  # "vehicle:{vehicle_id}:position"
    value: Dict[str, Any]  # {
                           #   "timestamp": 1615789200,
                           #   "position": {"x": 10.5, "y": 20.3, "z": 0.0},
                           #   "velocity": {"x": 5.0, "y": 0.0, "z": 0.0},
                           #   "heading": 90.0,
                           #   "vehicle_type": "car"
                           # }
```

**历史轨迹数据模型**：
```sql
-- TimescaleDB表结构
CREATE TABLE vehicle_trajectories (
    vehicle_id TEXT,
    timestamp TIMESTAMPTZ NOT NULL,
    position POINT,
    velocity POINT,
    heading FLOAT,
    vehicle_type TEXT,
    PRIMARY KEY (vehicle_id, timestamp)
);
-- 创建时间索引
SELECT create_hypertable('vehicle_trajectories', 'timestamp');
-- 创建空间索引
CREATE INDEX idx_vehicle_trajectories_position ON vehicle_trajectories USING GIST (position);
```

**碰撞风险数据模型**：
```sql
-- PostgreSQL表结构
CREATE TABLE collision_risks (
    risk_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    vehicle_id1 TEXT,
    vehicle_id2 TEXT,
    risk_level FLOAT,
    estimated_collision_time TIMESTAMPTZ,
    position GEOMETRY(POINT, 4326),
    details JSONB
);
```

## 5. 监控与告警系统设计

### 5.1 监控系统概述

监控系统负责收集、存储和分析系统运行指标，及时发现问题并触发告警。

### 5.2 监控指标设计

**系统级指标**：
- CPU使用率
- 内存使用率
- 网络吞吐量
- 磁盘I/O

**应用级指标**：
- 请求处理延迟
- 请求成功率
- 队列长度
- 处理吞吐量

**业务级指标**：
- 车辆数量
- 碰撞风险检测率
- 预警发送延迟
- 数据处理延迟

### 5.3 监控架构设计

**组件设计**：
- **指标收集器**：Prometheus客户端，收集各组件指标
- **指标存储**：Prometheus服务器，存储时间序列数据
- **可视化**：Grafana，提供监控面板
- **告警管理器**：Alertmanager，管理告警规则和通知

**实现框架**：
```python
from prometheus_client import Counter, Gauge, Histogram, Summary

# 定义指标
REQUEST_COUNT = Counter('request_count', 'Total request count', ['endpoint', 'status'])
PROCESSING_TIME = Histogram('processing_time_seconds', 'Time spent processing request', ['endpoint'])
QUEUE_SIZE = Gauge('queue_size', 'Current queue size', ['queue_name'])
VEHICLE_COUNT = Gauge('vehicle_count', 'Number of vehicles in the system', ['grid_id'])
COLLISION_RISK_COUNT = Counter('collision_risk_count', 'Number of collision risks detected', ['risk_level'])

# 使用示例
def process_request(endpoint):
    REQUEST_COUNT.labels(endpoint=endpoint, status='success').inc()
    with PROCESSING_TIME.labels(endpoint=endpoint).time():
        # 处理请求
        pass
```

### 5.4 告警策略设计

**告警规则**：
- **高延迟告警**：P99延迟超过阈值
- **错误率告警**：错误率超过阈值
- **资源告警**：CPU/内存使用率超过阈值
- **队列积压告警**：队列长度超过阈值

**告警级别**：
- **严重(Critical)**：需要立即处理的问题
- **警告(Warning)**：需要关注但不紧急的问题
- **信息(Info)**：提供系统状态信息

**告警通知渠道**：
- 电子邮件
- Slack/企业微信
- SMS短信
- 电话呼叫(严重告警)

**告警抑制与分组**：
- 相同类型告警分组
- 告警风暴抑制
- 告警静默期设置
