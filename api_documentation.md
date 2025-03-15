# API文档

## 1. 概述

本文档描述了分布式实时计算平台和3D碰撞检测系统的API接口。这些API允许外部系统与平台交互，包括数据提交、查询结果和管理操作。

## 2. 基础信息

### 2.1 基础URL

```
https://api.collision-detection-platform.com/v1
```

### 2.2 认证

所有API请求需要通过API密钥进行认证。API密钥应在HTTP请求头中提供：

```
Authorization: Bearer YOUR_API_KEY
```

### 2.3 响应格式

所有API响应均为JSON格式，包含以下基本结构：

```json
{
  "status": "success",
  "data": { ... },
  "message": "操作成功"
}
```

或者在发生错误时：

```json
{
  "status": "error",
  "error": {
    "code": "ERROR_CODE",
    "message": "错误描述"
  }
}
```

### 2.4 错误码

| 错误码 | 描述 |
|--------|------|
| 400 | 请求参数错误 |
| 401 | 认证失败 |
| 403 | 权限不足 |
| 404 | 资源不存在 |
| 429 | 请求过于频繁 |
| 500 | 服务器内部错误 |

## 3. 车辆位置API

### 3.1 提交车辆位置

**请求**

```
POST /vehicles/locations
```

**请求体**

```json
{
  "vehicle_id": "vehicle-123",
  "timestamp": 1647345678.123,
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
```

**响应**

```json
{
  "status": "success",
  "data": {
    "id": "loc-789012",
    "received_at": 1647345678.456
  },
  "message": "位置数据已接收"
}
```

### 3.2 批量提交车辆位置

**请求**

```
POST /vehicles/locations/batch
```

**请求体**

```json
{
  "locations": [
    {
      "vehicle_id": "vehicle-123",
      "timestamp": 1647345678.123,
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
      "timestamp": 1647345678.234,
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
  ]
}
```

**响应**

```json
{
  "status": "success",
  "data": {
    "received_count": 2,
    "received_at": 1647345678.567
  },
  "message": "批量位置数据已接收"
}
```

### 3.3 获取车辆当前位置

**请求**

```
GET /vehicles/{vehicle_id}/location
```

**响应**

```json
{
  "status": "success",
  "data": {
    "vehicle_id": "vehicle-123",
    "timestamp": 1647345678.123,
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
    "vehicle_type": "car",
    "updated_at": 1647345678.456
  },
  "message": "获取位置数据成功"
}
```

### 3.4 获取车辆历史轨迹

**请求**

```
GET /vehicles/{vehicle_id}/trajectory?start_time={start_time}&end_time={end_time}&interval={interval}
```

**参数**

- `start_time`: 开始时间戳（秒）
- `end_time`: 结束时间戳（秒）
- `interval`: 采样间隔（秒，可选，默认为1.0）

**响应**

```json
{
  "status": "success",
  "data": {
    "vehicle_id": "vehicle-123",
    "points": [
      {
        "timestamp": 1647345600.000,
        "position": {
          "x": 1200.00,
          "y": 750.00,
          "z": 0.0
        },
        "velocity": {
          "x": 10.0,
          "y": 5.0,
          "z": 0.0
        },
        "heading": 45.0
      },
      {
        "timestamp": 1647345601.000,
        "position": {
          "x": 1210.00,
          "y": 755.00,
          "z": 0.0
        },
        "velocity": {
          "x": 10.2,
          "y": 5.1,
          "z": 0.0
        },
        "heading": 45.0
      },
      // ... 更多轨迹点
    ],
    "count": 79
  },
  "message": "获取轨迹数据成功"
}
```

## 4. 碰撞风险API

### 4.1 获取车辆当前碰撞风险

**请求**

```
GET /vehicles/{vehicle_id}/collision-risks
```

**响应**

```json
{
  "status": "success",
  "data": {
    "vehicle_id": "vehicle-123",
    "timestamp": 1647345678.789,
    "risks": [
      {
        "risk_id": "risk-456789",
        "vehicle_id2": "vehicle-456",
        "risk_level": 0.85,
        "estimated_collision_time": 1647345683.456,
        "position": {
          "x": 1250.45,
          "y": 800.23,
          "z": 0.0
        },
        "relative_velocity": 15.7,
        "time_to_collision": 4.667
      },
      {
        "risk_id": "risk-567890",
        "vehicle_id2": "vehicle-789",
        "risk_level": 0.42,
        "estimated_collision_time": 1647345688.123,
        "position": {
          "x": 1270.34,
          "y": 810.56,
          "z": 0.0
        },
        "relative_velocity": 12.3,
        "time_to_collision": 9.334
      }
    ],
    "count": 2
  },
  "message": "获取碰撞风险数据成功"
}
```

### 4.2 获取区域内的碰撞风险

**请求**

```
GET /collision-risks/area?min_x={min_x}&min_y={min_y}&max_x={max_x}&max_y={max_y}&min_risk_level={min_risk_level}
```

**参数**

- `min_x`: 区域最小X坐标
- `min_y`: 区域最小Y坐标
- `max_x`: 区域最大X坐标
- `max_y`: 区域最大Y坐标
- `min_risk_level`: 最小风险等级（0.0-1.0，可选，默认为0.5）

**响应**

```json
{
  "status": "success",
  "data": {
    "timestamp": 1647345678.789,
    "risks": [
      {
        "risk_id": "risk-456789",
        "vehicle_id1": "vehicle-123",
        "vehicle_id2": "vehicle-456",
        "risk_level": 0.85,
        "estimated_collision_time": 1647345683.456,
        "position": {
          "x": 1250.45,
          "y": 800.23,
          "z": 0.0
        },
        "relative_velocity": 15.7,
        "time_to_collision": 4.667
      },
      // ... 更多风险数据
    ],
    "count": 12
  },
  "message": "获取区域碰撞风险数据成功"
}
```

### 4.3 订阅碰撞风险通知

**请求**

```
POST /vehicles/{vehicle_id}/collision-risks/subscribe
```

**请求体**

```json
{
  "callback_url": "https://your-server.com/webhook/collision-risks",
  "min_risk_level": 0.7,
  "max_time_to_collision": 10.0
}
```

**响应**

```json
{
  "status": "success",
  "data": {
    "subscription_id": "sub-123456",
    "vehicle_id": "vehicle-123",
    "created_at": 1647345678.789
  },
  "message": "碰撞风险订阅成功"
}
```

### 4.4 取消碰撞风险订阅

**请求**

```
DELETE /vehicles/{vehicle_id}/collision-risks/subscribe/{subscription_id}
```

**响应**

```json
{
  "status": "success",
  "data": {
    "subscription_id": "sub-123456",
    "vehicle_id": "vehicle-123",
    "deleted_at": 1647345678.789
  },
  "message": "碰撞风险订阅已取消"
}
```

## 5. 系统管理API

### 5.1 获取系统状态

**请求**

```
GET /system/status
```

**响应**

```json
{
  "status": "success",
  "data": {
    "timestamp": 1647345678.789,
    "system_status": "healthy",
    "components": {
      "data_ingestion": "healthy",
      "message_queue": "healthy",
      "compute_nodes": "healthy",
      "storage": "healthy",
      "api_service": "healthy"
    },
    "metrics": {
      "current_tps": 8765.4,
      "avg_latency_ms": 45.6,
      "p99_latency_ms": 98.7,
      "active_vehicles": 12345,
      "active_compute_nodes": 10,
      "storage_usage_percent": 67.8
    }
  },
  "message": "获取系统状态成功"
}
```

### 5.2 获取节点状态

**请求**

```
GET /system/nodes
```

**响应**

```json
{
  "status": "success",
  "data": {
    "timestamp": 1647345678.789,
    "nodes": [
      {
        "node_id": "node-123",
        "host": "compute-01.example.com",
        "port": 8080,
        "status": "active",
        "grid_ids": ["grid-123", "grid-124", "grid-125"],
        "load": 0.75,
        "capacity": 5000,
        "uptime": 86400.5
      },
      {
        "node_id": "node-456",
        "host": "compute-02.example.com",
        "port": 8080,
        "status": "active",
        "grid_ids": ["grid-126", "grid-127", "grid-128"],
        "load": 0.62,
        "capacity": 5000,
        "uptime": 43200.3
      },
      // ... 更多节点
    ],
    "count": 10
  },
  "message": "获取节点状态成功"
}
```

### 5.3 获取网格状态

**请求**

```
GET /system/grids
```

**响应**

```json
{
  "status": "success",
  "data": {
    "timestamp": 1647345678.789,
    "grids": [
      {
        "grid_id": "grid-123",
        "center": {
          "x": 1000.0,
          "y": 1000.0,
          "z": 0.0
        },
        "size": 100.0,
        "vehicle_count": 245,
        "density": 0.0245,
        "node_id": "node-123"
      },
      {
        "grid_id": "grid-124",
        "center": {
          "x": 1100.0,
          "y": 1000.0,
          "z": 0.0
        },
        "size": 100.0,
        "vehicle_count": 312,
        "density": 0.0312,
        "node_id": "node-123"
      },
      // ... 更多网格
    ],
    "count": 100
  },
  "message": "获取网格状态成功"
}
```

### 5.4 获取性能指标

**请求**

```
GET /system/metrics?start_time={start_time}&end_time={end_time}&interval={interval}
```

**参数**

- `start_time`: 开始时间戳（秒）
- `end_time`: 结束时间戳（秒）
- `interval`: 采样间隔（秒，可选，默认为60.0）

**响应**

```json
{
  "status": "success",
  "data": {
    "timestamp": 1647345678.789,
    "metrics": [
      {
        "timestamp": 1647345600.000,
        "tps": 8765.4,
        "avg_latency_ms": 45.6,
        "p95_latency_ms": 78.9,
        "p99_latency_ms": 98.7,
        "active_vehicles": 12345,
        "active_compute_nodes": 10,
        "cpu_usage_percent": 65.4,
        "memory_usage_percent": 78.9,
        "storage_usage_percent": 67.8
      },
      {
        "timestamp": 1647345660.000,
        "tps": 8932.1,
        "avg_latency_ms": 44.3,
        "p95_latency_ms": 76.5,
        "p99_latency_ms": 97.8,
        "active_vehicles": 12456,
        "active_compute_nodes": 10,
        "cpu_usage_percent": 67.2,
        "memory_usage_percent": 79.1,
        "storage_usage_percent": 68.0
      },
      // ... 更多指标
    ],
    "count": 60
  },
  "message": "获取性能指标成功"
}
```

## 6. 配置管理API

### 6.1 获取系统配置

**请求**

```
GET /system/config
```

**响应**

```json
{
  "status": "success",
  "data": {
    "grid_config": {
      "initial_size": 100.0,
      "min_size": 10.0,
      "max_size": 1000.0,
      "adjustment_threshold": 0.8,
      "adjustment_factor": 0.5
    },
    "node_config": {
      "max_workers": 16,
      "search_radius": 200.0,
      "batch_size": 100,
      "processing_interval": 0.1
    },
    "collision_detection_config": {
      "prediction_time": 10.0,
      "min_risk_level": 0.3,
      "max_distance": 500.0
    },
    "system_config": {
      "max_tps": 20000,
      "throttling_threshold": 0.9,
      "backup_interval": 3600.0,
      "log_level": "info"
    }
  },
  "message": "获取系统配置成功"
}
```

### 6.2 更新系统配置

**请求**

```
PUT /system/config
```

**请求体**

```json
{
  "grid_config": {
    "initial_size": 100.0,
    "min_size": 10.0,
    "max_size": 1000.0,
    "adjustment_threshold": 0.8,
    "adjustment_factor": 0.5
  },
  "node_config": {
    "max_workers": 16,
    "search_radius": 200.0,
    "batch_size": 100,
    "processing_interval": 0.1
  },
  "collision_detection_config": {
    "prediction_time": 10.0,
    "min_risk_level": 0.3,
    "max_distance": 500.0
  },
  "system_config": {
    "max_tps": 20000,
    "throttling_threshold": 0.9,
    "backup_interval": 3600.0,
    "log_level": "info"
  }
}
```

**响应**

```json
{
  "status": "success",
  "data": {
    "updated_at": 1647345678.789
  },
  "message": "系统配置更新成功"
}
```

## 7. 数据导出API

### 7.1 导出车辆轨迹数据

**请求**

```
GET /export/vehicles/{vehicle_id}/trajectory?start_time={start_time}&end_time={end_time}&format={format}
```

**参数**

- `start_time`: 开始时间戳（秒）
- `end_time`: 结束时间戳（秒）
- `format`: 导出格式（json, csv, 可选，默认为json）

**响应**

```json
{
  "status": "success",
  "data": {
    "export_id": "export-123456",
    "vehicle_id": "vehicle-123",
    "start_time": 1647345600.000,
    "end_time": 1647345900.000,
    "format": "json",
    "url": "https://api.collision-detection-platform.com/v1/export/download/export-123456",
    "expires_at": 1647349200.000
  },
  "message": "轨迹数据导出任务已创建"
}
```

### 7.2 导出碰撞风险数据

**请求**

```
GET /export/collision-risks?start_time={start_time}&end_time={end_time}&min_risk_level={min_risk_level}&format={format}
```

**参数**

- `start_time`: 开始时间戳（秒）
- `end_time`: 结束时间戳（秒）
- `min_risk_level`: 最小风险等级（0.0-1.0，可选，默认为0.5）
- `format`: 导出格式（json, csv, 可选，默认为json）

**响应**

```json
{
  "status": "success",
  "data": {
    "export_id": "export-234567",
    "start_time": 1647345600.000,
    "end_time": 1647345900.000,
    "min_risk_level": 0.5,
    "format": "json",
    "url": "https://api.collision-detection-platform.com/v1/export/download/export-234567",
    "expires_at": 1647349200.000
  },
  "message": "碰撞风险数据导出任务已创建"
}
```

### 7.3 下载导出数据

**请求**

```
GET /export/download/{export_id}
```

**响应**

文件下载（JSON或CSV格式）

## 8. 错误处理

### 8.1 请求参数错误

```json
{
  "status": "error",
  "error": {
    "code": "INVALID_PARAMETER",
    "message": "参数'min_risk_level'必须在0.0到1.0之间"
  }
}
```

### 8.2 资源不存在

```json
{
  "status": "error",
  "error": {
    "code": "RESOURCE_NOT_FOUND",
    "message": "找不到ID为'vehicle-999'的车辆"
  }
}
```

### 8.3 认证失败

```json
{
  "status": "error",
  "error": {
    "code": "AUTHENTICATION_FAILED",
    "message": "无效的API密钥"
  }
}
```

### 8.4 权限不足

```json
{
  "status": "error",
  "error": {
    "code": "PERMISSION_DENIED",
    "message": "没有权限访问此资源"
  }
}
```

### 8.5 请求过于频繁

```json
{
  "status": "error",
  "error": {
    "code": "RATE_LIMIT_EXCEEDED",
    "message": "请求过于频繁，请稍后再试",
    "retry_after": 60
  }
}
```

### 8.6 服务器内部错误

```json
{
  "status": "error",
  "error": {
    "code": "INTERNAL_SERVER_ERROR",
    "message": "服务器内部错误，请联系管理员",
    "reference_id": "err-345678"
  }
}
```

## 9. Webhook通知

当订阅的事件发生时，系统会向指定的回调URL发送HTTP POST请求。

### 9.1 碰撞风险通知

**请求体**

```json
{
  "event_type": "collision_risk",
  "timestamp": 1647345678.789,
  "subscription_id": "sub-123456",
  "data": {
    "risk_id": "risk-456789",
    "vehicle_id1": "vehicle-123",
    "vehicle_id2": "vehicle-456",
    "risk_level": 0.85,
    "estimated_collision_time": 1647345683.456,
    "position": {
      "x": 1250.45,
      "y": 800.23,
      "z": 0.0
    },
    "relative_velocity": 15.7,
    "time_to_collision": 4.667
  }
}
```

**响应**

Webhook接收方应返回HTTP 200状态码表示成功接收。

## 10. 限流策略

为了保护系统资源和确保服务质量，API实施了以下限流策略：

- 基本API密钥：每分钟100个请求
- 标准API密钥：每分钟1,000个请求
- 高级API密钥：每分钟10,000个请求
- 企业API密钥：每分钟100,000个请求

超过限制的请求将收到429状态码和RATE_LIMIT_EXCEEDED错误。

## 11. 版本控制

API使用URL路径中的版本号进行版本控制（例如，/v1/vehicles）。当API发生不兼容的变更时，将发布新版本（例如，/v2/vehicles）。旧版本将继续支持一段时间，但最终会被弃用和移除。

## 12. 最佳实践

- 使用批量API减少请求次数
- 实现指数退避重试策略处理临时错误
- 缓存不经常变化的数据
- 使用适当的查询参数减少返回的数据量
- 实现Webhook处理程序以接收实时通知
- 定期轮询系统状态API监控系统健康状况
