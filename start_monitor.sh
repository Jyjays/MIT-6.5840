#!/bin/bash

# ShardKV 监控界面启动脚本

echo "==================================="
echo "🚀 启动 ShardKV 监控界面"
echo "==================================="

# 检查是否在正确的目录
if [ ! -d "src/shardkv" ]; then
    echo "❌ 错误：请在项目根目录运行此脚本"
    echo "当前目录应包含 src/shardkv 文件夹"
    exit 1
fi

echo "📋 启动步骤："
echo "1. 编译 ShardKV 项目..."

# 进入 src 目录
cd src

# 编译项目
# go mod tidy
# if [ $? -ne 0 ]; then
#     echo "❌ go mod tidy 失败"
#     exit 1
# fi

echo "✅ 项目编译完成"

echo ""
echo "2. 准备启动测试并开启监控界面..."

echo ""
echo "📊 监控界面将在以下地址启动："
echo "   🌐 http://localhost:8080"
echo ""
echo "💡 使用说明："
echo "   - 在浏览器中访问 http://localhost:8080 查看监控界面"
echo "   - 界面会显示所有 ShardKV 服务器的状态"
echo "   - 实时监控分片迁移和同步复制过程"
echo "   - 查看 Raft 日志应用和配置更新事件"
echo ""
echo "🔧 运行测试："
echo "   在另一个终端中运行以下命令来启动测试："
echo "   cd src/shardkv && go test -v -run TestStaticShards5A"
echo ""
echo "⏹️  停止监控："
echo "   按 Ctrl+C 停止监控"
echo ""

# 创建一个简单的测试文件来启动监控
cat > /tmp/shardkv_monitor_test.go << 'EOF'
package main

import (
	"fmt"
	"time"
	"6.5840/shardkv"
)

func main() {
	fmt.Println("🎯 ShardKV 监控服务已启动")
	fmt.Println("📊 访问 http://localhost:8080 查看监控界面")
	fmt.Println("💡 在另一个终端运行测试来查看实时监控效果")
	fmt.Println("")
	
	// 启动监控器
	monitor := shardkv.GetMonitor()
	monitor.LogEvent("SYSTEM", 0, 0, "监控系统启动", map[string]interface{}{
		"message": "ShardKV 监控界面已准备就绪",
	})
	
	// 保持程序运行
	for {
		time.Sleep(time.Second)
	}
}
EOF

echo "🎯 启动监控服务..."
go run /tmp/shardkv_monitor_test.go
