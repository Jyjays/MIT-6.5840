package main

import (
	"strconv"
	"strings"

	"6.5840/mr"
)

// Count the movies that users really like
// Map function should return a slice of user's favorite movies
func Map(filename string, content string) []mr.KeyValue {
	kva := []mr.KeyValue{}
	// content: userid,movieid,rating\n
	// Split by \n
	content = strings.Replace(content, "\r\n", "\n", -1)
	lines := strings.Split(content, "\n")
	for _, line := range lines {
		parts := strings.Split(line, ",")
		if len(parts) != 3 {
			continue
		}
		userID, err1 := strconv.Atoi(parts[0])
		movieID, err2 := strconv.Atoi(parts[1])
		rating, err3 := strconv.ParseFloat(parts[2], 64)
		if err1 != nil || err2 != nil || err3 != nil {
			continue
		}
		if rating >= 0 {
			kva = append(kva, mr.KeyValue{Key: strconv.Itoa(userID), Value: strconv.Itoa(movieID)})
		}
	}
	return kva
}

func Reduce(key string, values []string) string {
	if len(values) > 10 {
		values = values[:10]
	}
	return strings.Join(values, ",")
}
