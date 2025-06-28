# ShardKV 可视化监控界面

这是一个为 MIT 6.5840 ShardKV 项目开发的实时可视化监控界面，可以展示服务端内部的同步复制过程。

## 🌟 功能特性

### 📊 实时监控
- **服务器状态**: 显示每个 ShardKV 服务器的实时状态，包括是否为 Leader、当前 Term、配置版本等
- **分片状态**: 可视化展示所有分片的状态（Serving、Pulling、Sending、GCing）
- **事件流**: 实时显示系统中发生的所有重要事件

### 🔍 监控内容
- **Raft 同步**: 日志复制、快照应用、Leader 选举
- **分片迁移**: 分片拉取、发送、删除过程
- **配置更新**: 配置变更和分片重分配
- **客户端请求**: Get、Put、Append 操作的处理过程

### 🎨 界面特色
- **响应式设计**: 适配不同屏幕尺寸
- **实时更新**: 每2秒自动刷新数据
- **颜色编码**: 不同状态用不同颜色区分
- **详细信息**: 点击可查看事件详情

## 🚀 快速开始

### 1. 启动监控界面
```bash
# 在项目根目录执行
./start_monitor.sh
```

监控界面将在 http://localhost:8080 启动。

### 2. 运行测试查看效果
在另一个终端中：
```bash
# 运行演示脚本
./run_demo.sh

# 或直接运行特定测试
cd src/shardkv
go test -v -run TestJoinLeave5B
```

## 🎯 监控界面说明

### 服务器状态面板
- **绿色边框**: Leader 服务器
- **灰色边框**: Follower 服务器  
- **分片显示**: 每个分片显示为小方块，包含分片ID和键值对数量
- **状态颜色**:
  - 🟢 Serving (绿色) - 正常服务
  - 🟡 Pulling (黄色) - 正在拉取数据
  - 🔵 Sending (蓝色) - 正在发送数据
  - 🔴 GCing (红色) - 等待垃圾回收

### 事件流面板
事件按时间顺序显示，不同类型用不同背景色：
- **RAFT** (蓝色) - Raft 相关事件
- **SHARD** (紫色) - 分片迁移事件
- **CONFIG** (绿色) - 配置变更事件
- **CLIENT** (橙色) - 客户端请求事件

### 集群统计面板
显示整个集群的统计信息：
- 总服务器数量
- Leader 数量
- 活跃分片数
- 迁移中分片数

## 🔧 技术实现

### 监控架构
```
ShardKV Server → Monitor → Web Server → Browser
     ↓              ↓         ↓           ↓
   事件记录      状态收集   HTTP API   可视化界面
```

### 核心组件
- **Monitor**: 事件收集器和状态管理器
- **EventLogger**: 在关键操作点记录事件
- **WebServer**: 提供 HTTP API 和 Web 界面
- **Frontend**: 基于 HTML/CSS/JavaScript 的前端界面

### 监控点
监控系统在以下关键位置插入了事件记录：
- 客户端请求处理 (Get/Put/Append)
- Raft 日志应用和快照
- 配置变更检测和处理
- 分片状态转换
- 分片数据迁移 (拉取/发送)
- 分片垃圾回收

## 📁 文件结构

```
src/shardkv/
├── monitor.go          # 监控核心实现
├── server.go           # ShardKV 服务器 (已添加监控)
├── applier.go          # 日志应用器 (已添加监控)
├── ...                 # 其他原有文件
start_monitor.sh        # 监控启动脚本
run_demo.sh            # 演示测试脚本
README_MONITOR.md      # 本说明文件
```

## 🎮 使用场景

### 学习分布式系统
- 观察 Raft 共识算法的工作过程
- 理解分片分布式存储的实现
- 学习配置变更和数据迁移机制

### 调试和优化
- 发现性能瓶颈
- 调试分片迁移问题
- 分析并发冲突

### 演示和展示
- 可视化展示分布式系统运行过程
- 教学演示工具
- 项目展示

## 🔗 API 接口

监控界面提供以下 REST API：

- `GET /` - 主监控页面
- `GET /api/events` - 获取事件列表
- `GET /api/servers` - 获取服务器状态
- `GET /api/status` - 获取集群统计

## 🎨 自定义和扩展

### 添加新的监控事件
```go
monitor := GetMonitor()
monitor.LogEvent("CUSTOM", groupId, serverId, "描述", map[string]interface{}{
    "key": "value",
})
```

### 修改界面样式
编辑 `monitor.go` 中的 HTML/CSS 部分来自定义界面外观。

### 添加新的统计指标
在 `handleStatus` 函数中添加新的统计逻辑。

## 📝 注意事项

- 监控界面仅用于开发和测试环境
- 大量事件可能影响性能，生产环境请谨慎使用
- 默认监控端口为 8080，确保端口未被占用
- 事件存储在内存中，重启后会丢失历史数据

## 🤝 贡献

欢迎提交 Issue 和 Pull Request 来改进这个监控界面！

---

Happy monitoring! 🎉
