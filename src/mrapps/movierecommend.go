package main

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"6.5840/mr"
)

var userMovies map[int]map[int]float64

func init() {
	userMovies = make(map[int]map[int]float64)
	data, err := os.ReadFile("/home/jyjays/MIT6.5840/6.5840/src/main/rd_res/trainSet.csv")
	if err != nil {
		panic(err)
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		fields := strings.Split(line, ",")
		if len(fields) != 3 {
			continue
		}
		userID, err1 := strconv.Atoi(fields[0])
		movieID, err2 := strconv.Atoi(fields[1])
		rating, err3 := strconv.ParseFloat(fields[2], 64)
		if err1 != nil || err2 != nil || err3 != nil {
			continue
		}
		if _, exists := userMovies[userID]; !exists {
			userMovies[userID] = make(map[int]float64)
		}
		userMovies[userID][movieID] = rating
	}
}

func getUserMovies(userID int) (map[int]float64, error) {
	movies, exists := userMovies[userID]
	if !exists {
		return nil, fmt.Errorf("User %d not found", userID)
	}
	return movies, nil
}

func Map(filename string, content string) []mr.KeyValue {
	kva := []mr.KeyValue{}
	// content: user1,user2 ,similarity\n
	// Split by \n
	// recommend movie for one user
	lines := strings.Split(content, "\n")
	for _, line := range lines {
		parts := strings.Split(line, ",")
		if len(parts) != 3 {
			continue
		}
		userID, err1 := strconv.Atoi(strings.TrimSpace(parts[0]))
		relatedUserID, err2 := strconv.Atoi(strings.TrimSpace(parts[1]))
		similarity, err3 := strconv.ParseFloat(strings.TrimSpace(parts[2]), 64)
		if err1 != nil || err2 != nil || err3 != nil {
			continue
		}
		kva = append(kva, mr.KeyValue{Key: strconv.Itoa(userID), Value: fmt.Sprintf("%d,%f", relatedUserID, similarity)})
	}
	return kva
}

func Reduce(key string, values []string) string {
	// key: userID
	// values: relatedUserID,similarity
	relatedUserSimilarity := make(map[int]float64)
	for _, value := range values {
		parts := strings.Split(value, ",")
		if len(parts) != 2 {
			continue
		}
		relatedUserID, err1 := strconv.Atoi(parts[0])
		similarity, err2 := strconv.ParseFloat(parts[1], 64)
		if err1 != nil || err2 != nil {
			continue
		}
		relatedUserSimilarity[relatedUserID] = similarity
	}

	// 获取目标用户的观影记录
	userID, err := strconv.Atoi(key)
	if err != nil {
		return ""
	}
	userMovies, err := getUserMovies(userID) // 当前用户看过的电影集合
	if err != nil {
		return ""
	}

	// 获取相关用户的观影记录
	relatedUserMovie := make(map[int]map[int]float64)
	for relatedUserID := range relatedUserSimilarity {
		movies, err := getUserMovies(relatedUserID)
		if err != nil {
			continue
		}
		relatedUserMovie[relatedUserID] = movies
	}

	// 计算推荐分数
	recommendations := make(map[int]float64)
	for relatedUserID, movies := range relatedUserMovie {
		for movieID, rating := range movies {
			// 检查目标用户是否已经看过该电影
			if _, watched := userMovies[movieID]; !watched {
				recommendations[movieID] += relatedUserSimilarity[relatedUserID] * rating
			}
		}
	}

	// 收集推荐结果并排序
	type movieRecommendation struct {
		movieID int
		score   float64
	}
	var recs []movieRecommendation
	for movieID, score := range recommendations {
		recs = append(recs, movieRecommendation{movieID, score})
	}
	sort.Slice(recs, func(i, j int) bool {
		return recs[i].score > recs[j].score
	})

	// 选取前10个推荐电影
	topN := 10
	if len(recs) < topN {
		topN = len(recs)
	}
	recommendedMovies := make([]string, topN)
	for i := 0; i < topN; i++ {
		recommendedMovies[i] = strconv.Itoa(recs[i].movieID)
	}
	return strings.Join(recommendedMovies, ";")
}
