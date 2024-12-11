package main

import (
	"strings"

	"6.5840/mr"
)

// Map function for processing the rating data
func Map(key string, value string) []mr.KeyValue {
	value = strings.Replace(value, "\r\n", "\n", -1) // 将 \r\n 转换为 \n
	lines := strings.Split(value, "\n")
	kva := []mr.KeyValue{}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		fields := strings.Split(line, ",")
		if len(fields) < 3 {
			continue
		}
		userID := fields[0]
		movieID := fields[1]
		rating := fields[2]
		kv := mr.KeyValue{Key: movieID, Value: userID + "," + rating}
		kva = append(kva, kv)
	}
	return kva
}

// Reduce function for generating the inverted index
func Reduce(key string, values []string) string {
	return strings.Join(values, ",")
}
