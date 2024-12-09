package main

import (
	"strings"

	"6.5840/mr"
)

// Map function for processing the rating data
func Map(key string, value string) []mr.KeyValue {
	// 检查是否是空行，避免处理无效数据
	value = strings.Replace(value, "\r\n", "\n", -1) // 将 \r\n 转换为 \n
	// 按行拆分数据
	lines := strings.Split(value, "\n")
	kva := []mr.KeyValue{}
	// 遍历每一行数据
	for _, line := range lines {
		// 去除行首尾空白字符
		line = strings.TrimSpace(line)
		// 跳过空行
		if len(line) == 0 {
			continue
		}

		fields := strings.Split(line, ",")
		if len(fields) < 3 {
			continue
		}

		// 提取 userID 和每个 movieID 和 rating
		userID := fields[0]
		movieID := fields[1]
		rating := fields[2]

		kv := mr.KeyValue{Key: movieID, Value: userID + "," + rating}
		kva = append(kva, kv)
	}

	// 返回所有生成的键值对
	return kva
}

// Reduce function for generating the inverted index
func Reduce(key string, values []string) string {
	// 只返回 values 部分的字符串，避免包含 key
	return strings.Join(values, ",")
}
