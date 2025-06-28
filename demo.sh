#!/bin/bash

echo "🎯 ShardKV 监控界面演示"
echo "=============================="
echo ""

# 检查端口是否已被占用
if lsof -Pi :8080 -sTCP:LISTEN -t >/dev/null ; then
    echo "⚠️  端口 8080 已被占用，正在尝试结束占用进程..."
    pkill -f "monitor_demo.go"
    sleep 2
fi

echo "🚀 启动监控服务..."
cd src

# 后台启动监控服务
nohup go run monitor_demo.go > /tmp/monitor.log 2>&1 &
MONITOR_PID=$!

echo "⏳ 等待监控服务启动..."
sleep 3

# 检查监控服务是否成功启动
if ! curl -s http://localhost:8080 > /dev/null; then
    echo "❌ 监控服务启动失败"
    exit 1
fi

echo "✅ 监控服务已启动在 http://localhost:8080"
echo ""
echo "🌐 正在打开监控界面..."

# 尝试打开浏览器（根据系统选择合适的命令）
if command -v xdg-open > /dev/null; then
    xdg-open http://localhost:8080 &
elif command -v open > /dev/null; then
    open http://localhost:8080 &
elif command -v firefox > /dev/null; then
    firefox http://localhost:8080 &
elif command -v chromium-browser > /dev/null; then
    chromium-browser http://localhost:8080 &
else
    echo "请手动在浏览器中打开: http://localhost:8080"
fi

echo ""
echo "📋 演示选项："
echo "1. 🧪 运行静态分片测试 (TestStaticShards5A)"
echo "2. 🔄 运行分片迁移测试 (TestJoinLeave5B)" 
echo "3. 📸 运行快照测试 (TestSnapshot5B)"
echo "4. ⚡ 运行并发测试 (TestConcurrent1_5B)"
echo "5. 🎯 运行所有测试"
echo "6. 🛑 停止演示"
echo ""

while true; do
    read -p "请选择演示选项 (1-6): " choice
    
    case $choice in
        1)
            echo "🧪 运行静态分片测试..."
            cd shardkv
            go test -v -run TestStaticShards5A
            cd ..
            ;;
        2)
            echo "🔄 运行分片迁移测试..."
            cd shardkv
            go test -v -run TestJoinLeave5B
            cd ..
            ;;
        3)
            echo "📸 运行快照测试..."
            cd shardkv
            go test -v -run TestSnapshot5B
            cd ..
            ;;
        4)
            echo "⚡ 运行并发测试..."
            cd shardkv
            go test -v -run TestConcurrent1_5B
            cd ..
            ;;
        5)
            echo "🎯 运行所有测试..."
            cd shardkv
            go test -v
            cd ..
            ;;
        6)
            echo "🛑 停止演示..."
            kill $MONITOR_PID 2>/dev/null
            pkill -f "monitor_demo.go" 2>/dev/null
            echo "✅ 演示已停止"
            exit 0
            ;;
        *)
            echo "❌ 无效选择，请输入 1-6"
            ;;
    esac
    
    echo ""
    echo "💡 测试完成后，请查看监控界面查看详细的执行过程"
    echo "🔄 刷新浏览器页面来查看最新的监控数据"
    echo ""
done
