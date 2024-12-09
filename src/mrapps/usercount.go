package main

import (
	"strconv"
	"strings"

	"6.5840/mr"
)

func Map(filename string, content string) []mr.KeyValue {
	content = strings.Replace(content, "\r\n", "\n", -1) // 将 \r\n 转换为 \n
	// 按行拆分数据
	lines := strings.Split(content, "\n")
	kva := []mr.KeyValue{}

	for _, line := range lines {
		line = strings.TrimSpace(line)
		// 跳过空行
		if len(line) == 0 {
			continue
		}

		fields := strings.Split(line, ",")
		if len(fields) < 3 {
			continue
		}

		userId := fields[0]
		// Emit a key-value pair (userId, "1")
		kva = append(kva, mr.KeyValue{
			Key:   userId,
			Value: "1",
		})
	}
	return kva
}

func Reduce(key string, value []string) string {
	// Key: userId
	// Values: ["1", "1", "1", ...]
	// Count the number of values
	return strconv.Itoa(len(value))
}
