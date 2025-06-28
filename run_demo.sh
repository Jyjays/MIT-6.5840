#!/bin/bash

# ShardKV 演示脚本 - 在有监控界面时运行测试

echo "🧪 启动 ShardKV 演示测试"
echo "确保监控界面已在 http://localhost:8080 启动"
echo ""

cd src/shardkv

echo "📋 可用的测试："
echo "1. TestStaticShards5A - 静态分片测试"
echo "2. TestJoinLeave5B - 分片迁移测试"
echo "3. TestSnapshot5B - 快照测试"
echo "4. TestConcurrent1_5B - 并发测试"
echo ""

read -p "选择要运行的测试 (1-4) 或输入 'all' 运行所有测试: " choice

case $choice in
    1)
        echo "🔬 运行静态分片测试..."
        go test -v -run TestStaticShards5A -race
        ;;
    2)
        echo "🔄 运行分片迁移测试..."
        go test -v -run TestJoinLeave5B -race
        ;;
    3)
        echo "📸 运行快照测试..."
        go test -v -run TestSnapshot5B -race
        ;;
    4)
        echo "⚡ 运行并发测试..."
        go test -v -run TestConcurrent1_5B -race
        ;;
    all)
        echo "🎯 运行所有测试..."
        go test -v -race
        ;;
    *)
        echo "❌ 无效选择"
        exit 1
        ;;
esac

echo ""
echo "✅ 测试完成！查看监控界面了解详细的执行过程。"
