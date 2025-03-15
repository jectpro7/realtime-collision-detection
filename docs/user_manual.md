# 用户手册

## 1. 简介

欢迎使用分布式实时计算平台和3D碰撞检测系统。本手册将指导您如何使用系统的各项功能，包括数据提交、查询结果、配置系统和监控性能。

## 2. 系统概述

分布式实时计算平台是一个高性能、高可靠性的分布式系统，专为处理大规模实时数据流和计算任务而设计。基于该平台，我们实现了一个3D风险分析系统，用于检测电动汽车之间的潜在碰撞风险。

系统主要功能包括：

- 接收和处理车辆位置报告
- 执行实时碰撞检测算法
- 预测潜在碰撞风险
- 发送碰撞风险预警
- 提供数据查询和分析接口
- 支持系统监控和管理

## 3. 快速入门

### 3.1 系统要求

- 支持现代浏览器（Chrome, Firefox, Safari, Edge）
- 网络连接
- API密钥（联系管理员获取）

### 3.2 访问系统

1. 打开浏览器，访问系统管理控制台：`https://console.collision-detection-platform.com`
2. 使用您的用户名和密码登录
3. 首次登录时，系统会要求您修改密码并设置双因素认证

### 3.3 基本操作流程

1. 配置数据源，设置车辆位置数据的提交方式
2. 配置碰撞检测参数，如风险等级阈值、预测时间等
3. 设置通知方式，如邮件、短信、Webhook等
4. 监控系统运行状态和性能指标
5. 查询和分析碰撞风险数据

## 4. 数据提交

### 4.1 通过API提交数据

系统提供了RESTful API，允许您以编程方式提交车辆位置数据。详细的API文档请参考[API文档](./api_documentation.md)。

基本的数据提交示例：

```python
import requests
import json
import time

# API基础URL
base_url = "https://api.collision-detection-platform.com/v1"

# API密钥
api_key = "YOUR_API_KEY"

# 请求头
headers = {
    "Authorization": f"Bearer {api_key}",
    "Content-Type": "application/json"
}

# 车辆位置数据
location_data = {
    "vehicle_id": "vehicle-123",
    "timestamp": time.time(),
    "position": {
        "x": 1234.56,
        "y": 789.01,
        "z": 0.0
    },
    "velocity": {
        "x": 10.5,
        "y": 5.2,
        "z": 0.0
    },
    "heading": 45.0,
    "vehicle_type": "car"
}

# 发送请求
response = requests.post(
    f"{base_url}/vehicles/locations",
    headers=headers,
    data=json.dumps(location_data)
)

# 处理响应
if response.status_code == 200:
    result = response.json()
    print(f"位置数据已接收，ID: {result['data']['id']}")
else:
    print(f"错误: {response.status_code}, {response.text}")
```

### 4.2 批量提交数据

对于大量数据，建议使用批量API减少请求次数：

```python
# 批量车辆位置数据
batch_data = {
    "locations": [
        {
            "vehicle_id": "vehicle-123",
            "timestamp": time.time(),
            "position": {
                "x": 1234.56,
                "y": 789.01,
                "z": 0.0
            },
            "velocity": {
                "x": 10.5,
                "y": 5.2,
                "z": 0.0
            },
            "heading": 45.0,
            "vehicle_type": "car"
        },
        {
            "vehicle_id": "vehicle-456",
            "timestamp": time.time(),
            "position": {
                "x": 2345.67,
                "y": 890.12,
                "z": 0.0
            },
            "velocity": {
                "x": -8.3,
                "y": 4.1,
                "z": 0.0
            },
            "heading": 270.0,
            "vehicle_type": "truck"
        }
        # 可以包含更多车辆位置数据
    ]
}

# 发送批量请求
response = requests.post(
    f"{base_url}/vehicles/locations/batch",
    headers=headers,
    data=json.dumps(batch_data)
)
```

### 4.3 使用SDK提交数据

我们提供了多种语言的SDK，简化数据提交过程：

**Python SDK示例**：

```python
from collision_detection_sdk import CollisionDetectionClient

# 创建客户端
client = CollisionDetectionClient(api_key="YOUR_API_KEY")

# 提交单个车辆位置
client.submit_vehicle_location(
    vehicle_id="vehicle-123",
    position=(1234.56, 789.01, 0.0),
    velocity=(10.5, 5.2, 0.0),
    heading=45.0,
    vehicle_type="car"
)

# 批量提交车辆位置
locations = [
    {
        "vehicle_id": "vehicle-123",
        "position": (1234.56, 789.01, 0.0),
        "velocity": (10.5, 5.2, 0.0),
        "heading": 45.0,
        "vehicle_type": "car"
    },
    {
        "vehicle_id": "vehicle-456",
        "position": (2345.67, 890.12, 0.0),
        "velocity": (-8.3, 4.1, 0.0),
        "heading": 270.0,
        "vehicle_type": "truck"
    }
]
client.submit_vehicle_locations_batch(locations)
```

### 4.4 数据格式要求

车辆位置数据必须包含以下字段：

- `vehicle_id`：车辆唯一标识符（字符串）
- `timestamp`：时间戳，Unix时间（秒）（浮点数）
- `position`：位置坐标，包含x、y、z三个分量（浮点数）
- `velocity`：速度向量，包含x、y、z三个分量（浮点数）
- `heading`：航向角，单位为度，0-360（浮点数）
- `vehicle_type`：车辆类型，如"car"、"truck"等（字符串）

## 5. 查询结果

### 5.1 查询车辆位置

您可以通过API查询车辆的当前位置和历史轨迹：

```python
# 查询车辆当前位置
response = requests.get(
    f"{base_url}/vehicles/vehicle-123/location",
    headers=headers
)

# 查询车辆历史轨迹
response = requests.get(
    f"{base_url}/vehicles/vehicle-123/trajectory?start_time=1647345600&end_time=1647345900&interval=1.0",
    headers=headers
)
```

### 5.2 查询碰撞风险

您可以查询特定车辆的碰撞风险或特定区域内的碰撞风险：

```python
# 查询车辆当前碰撞风险
response = requests.get(
    f"{base_url}/vehicles/vehicle-123/collision-risks",
    headers=headers
)

# 查询区域内的碰撞风险
response = requests.get(
    f"{base_url}/collision-risks/area?min_x=1000&min_y=1000&max_x=2000&max_y=2000&min_risk_level=0.7",
    headers=headers
)
```

### 5.3 使用管理控制台查询

除了API，您还可以使用管理控制台查询结果：

1. 登录管理控制台
2. 导航到"数据查询"页面
3. 选择查询类型（车辆位置、历史轨迹、碰撞风险等）
4. 输入查询参数
5. 点击"查询"按钮
6. 查看结果，可以选择导出为CSV或JSON格式

### 5.4 数据可视化

管理控制台提供了丰富的数据可视化功能：

1. 车辆位置地图：显示车辆在地图上的实时位置
2. 轨迹回放：回放车辆历史轨迹
3. 热力图：显示车辆密度分布
4. 风险地图：显示碰撞风险分布
5. 统计图表：显示各种统计数据和趋势

## 6. 接收通知

### 6.1 配置Webhook

您可以通过Webhook接收实时碰撞风险通知：

```python
# 订阅碰撞风险通知
subscription_data = {
    "callback_url": "https://your-server.com/webhook/collision-risks",
    "min_risk_level": 0.7,
    "max_time_to_collision": 10.0
}

response = requests.post(
    f"{base_url}/vehicles/vehicle-123/collision-risks/subscribe",
    headers=headers,
    data=json.dumps(subscription_data)
)
```

您需要在您的服务器上实现一个Webhook处理程序，接收和处理通知：

```python
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/webhook/collision-risks', methods=['POST'])
def handle_collision_risk():
    data = request.json
    
    # 处理碰撞风险通知
    event_type = data.get('event_type')
    timestamp = data.get('timestamp')
    risk_data = data.get('data')
    
    # 执行您的业务逻辑，如发送警报、记录日志等
    print(f"收到碰撞风险通知: {risk_data}")
    
    # 返回200表示成功接收
    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
```

### 6.2 配置邮件通知

您可以在管理控制台配置邮件通知：

1. 登录管理控制台
2. 导航到"通知设置"页面
3. 点击"添加邮件通知"
4. 输入收件人邮箱、通知条件（如风险等级阈值）
5. 点击"保存"

### 6.3 配置短信通知

类似地，您可以配置短信通知：

1. 登录管理控制台
2. 导航到"通知设置"页面
3. 点击"添加短信通知"
4. 输入接收手机号码、通知条件
5. 点击"保存"

## 7. 系统配置

### 7.1 配置碰撞检测参数

您可以通过API或管理控制台配置碰撞检测参数：

```python
# 更新系统配置
config_data = {
    "collision_detection_config": {
        "prediction_time": 10.0,
        "min_risk_level": 0.3,
        "max_distance": 500.0
    }
}

response = requests.put(
    f"{base_url}/system/config",
    headers=headers,
    data=json.dumps(config_data)
)
```

在管理控制台中：

1. 登录管理控制台
2. 导航到"系统配置"页面
3. 修改碰撞检测参数
4. 点击"保存"

### 7.2 配置系统资源

管理员可以配置系统资源分配：

1. 登录管理控制台（管理员账户）
2. 导航到"资源管理"页面
3. 调整计算节点数量、内存分配等
4. 点击"应用"

### 7.3 配置用户权限

管理员可以管理用户和权限：

1. 登录管理控制台（管理员账户）
2. 导航到"用户管理"页面
3. 添加/编辑用户，分配角色和权限
4. 点击"保存"

## 8. 监控系统

### 8.1 查看系统状态

您可以通过API或管理控制台查看系统状态：

```python
# 获取系统状态
response = requests.get(
    f"{base_url}/system/status",
    headers=headers
)
```

在管理控制台中：

1. 登录管理控制台
2. 导航到"系统状态"页面
3. 查看各组件状态和健康指标

### 8.2 查看性能指标

您可以查看系统性能指标：

```python
# 获取性能指标
response = requests.get(
    f"{base_url}/system/metrics?start_time=1647345600&end_time=1647345900&interval=60",
    headers=headers
)
```

在管理控制台中：

1. 登录管理控制台
2. 导航到"性能监控"页面
3. 查看吞吐量、延迟、资源使用率等指标
4. 可以调整时间范围和刷新间隔

### 8.3 设置告警

您可以配置性能告警：

1. 登录管理控制台
2. 导航到"告警设置"页面
3. 点击"添加告警规则"
4. 选择指标（如P99延迟）、阈值和通知方式
5. 点击"保存"

### 8.4 查看日志

您可以查看系统日志：

1. 登录管理控制台
2. 导航到"日志查询"页面
3. 选择日志类型、时间范围和过滤条件
4. 点击"查询"
5. 查看日志内容，可以导出日志文件

## 9. 数据导出

### 9.1 导出车辆轨迹

您可以导出车辆轨迹数据：

```python
# 导出车辆轨迹
response = requests.get(
    f"{base_url}/export/vehicles/vehicle-123/trajectory?start_time=1647345600&end_time=1647345900&format=csv",
    headers=headers
)

export_data = response.json()
export_url = export_data['data']['url']

# 下载导出文件
download_response = requests.get(export_url, headers=headers)
with open("trajectory_data.csv", "wb") as f:
    f.write(download_response.content)
```

在管理控制台中：

1. 登录管理控制台
2. 导航到"数据导出"页面
3. 选择"车辆轨迹"
4. 输入车辆ID、时间范围和导出格式
5. 点击"导出"
6. 等待导出完成后，点击"下载"

### 9.2 导出碰撞风险数据

类似地，您可以导出碰撞风险数据：

```python
# 导出碰撞风险数据
response = requests.get(
    f"{base_url}/export/collision-risks?start_time=1647345600&end_time=1647345900&min_risk_level=0.5&format=json",
    headers=headers
)
```

在管理控制台中：

1. 登录管理控制台
2. 导航到"数据导出"页面
3. 选择"碰撞风险"
4. 输入时间范围、最小风险等级和导出格式
5. 点击"导出"
6. 等待导出完成后，点击"下载"

## 10. 故障排除

### 10.1 常见问题

**问题**：数据提交失败，返回401错误
**解决方案**：检查API密钥是否正确，是否已过期

**问题**：数据提交成功，但查询不到结果
**解决方案**：检查数据格式是否正确，特别是时间戳和坐标格式

**问题**：系统性能下降
**解决方案**：检查数据提交速率，可能超过系统配置的限制；联系管理员增加资源

**问题**：没有收到碰撞风险通知
**解决方案**：检查Webhook URL是否正确，服务器是否可访问；检查风险等级阈值设置

### 10.2 联系支持

如果您遇到无法解决的问题，请联系技术支持：

- 邮件：support@collision-detection-platform.com
- 电话：+1-234-567-8900
- 在线支持：登录管理控制台，点击右下角"在线支持"按钮

请提供以下信息以便我们更好地帮助您：

- 您的账户ID
- 问题描述
- 错误消息和代码
- 重现步骤
- 日志文件（如果有）

## 11. 最佳实践

### 11.1 数据提交

- 使用批量API减少请求次数
- 实现本地缓冲，在网络中断时存储数据
- 使用压缩减少数据传输量
- 实现指数退避重试策略处理临时错误

### 11.2 系统配置

- 根据车辆密度调整网格大小
- 根据实际需求调整预测时间
- 设置合理的风险等级阈值，避免过多或过少的通知
- 定期检查和优化配置

### 11.3 资源管理

- 监控资源使用情况，及时扩展资源
- 在预期流量高峰前增加资源
- 使用自动扩展功能应对流量波动
- 定期清理不再需要的历史数据

### 11.4 安全性

- 定期更换API密钥
- 使用HTTPS进行所有API通信
- 实施IP白名单限制API访问
- 启用双因素认证保护管理控制台
- 定期审查用户权限

## 12. 附录

### 12.1 API参考

详细的API文档请参考[API文档](./api_documentation.md)。

### 12.2 错误码参考

| 错误码 | 描述 | 可能的原因 | 解决方案 |
|--------|------|------------|----------|
| 400 | 请求参数错误 | 参数格式不正确或缺少必要参数 | 检查请求参数格式和必要字段 |
| 401 | 认证失败 | API密钥无效或已过期 | 检查API密钥，必要时重新生成 |
| 403 | 权限不足 | 当前用户没有执行该操作的权限 | 联系管理员获取适当的权限 |
| 404 | 资源不存在 | 请求的资源（如车辆ID）不存在 | 检查资源ID是否正确 |
| 429 | 请求过于频繁 | 超过API调用频率限制 | 减少请求频率，实现请求缓冲 |
| 500 | 服务器内部错误 | 服务器处理请求时发生错误 | 联系技术支持，提供错误详情 |

### 12.3 术语表

| 术语 | 定义 |
|------|------|
| TPS | 每秒事务处理量，衡量系统处理能力的指标 |
| P99延迟 | 99%请求的响应时间低于此值 |
| 风险等级 | 碰撞风险的严重程度，范围0.0-1.0 |
| 预测时间 | 系统预测未来可能碰撞的时间范围 |
| 网格 | 空间索引的基本单位，用于高效查找附近车辆 |
| Webhook | 允许系统向外部服务器发送实时通知的机制 |

### 12.4 SDK下载

- [Python SDK](https://github.com/collision-detection-platform/python-sdk)
- [Java SDK](https://github.com/collision-detection-platform/java-sdk)
- [JavaScript SDK](https://github.com/collision-detection-platform/js-sdk)
- [Go SDK](https://github.com/collision-detection-platform/go-sdk)
- [C# SDK](https://github.com/collision-detection-platform/csharp-sdk)

### 12.5 示例代码

更多示例代码请访问我们的[GitHub仓库](https://github.com/collision-detection-platform/examples)。
