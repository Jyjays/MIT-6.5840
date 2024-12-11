package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"6.5840/mr"
)

// getUserRatingCount fetches the rating count for a given user from the preloaded data
var userRatingCounts map[string]int

func init() {
	userRatingCounts = make(map[string]int)

	// merged_output.txt contains the user rating count data
	data, err := os.ReadFile("/home/jyjays/MIT6.5840/6.5840/src/main/rd_res/user_rating_count.txt")
	if err != nil {
		panic(err)
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		fields := strings.Split(line, " ")
		if len(fields) != 2 {
			continue
		}
		userID := fields[0]
		count, err := strconv.Atoi(fields[1])

		if err != nil {
			continue
		}
		userRatingCounts[userID] = count
	}
}

// getUserRatingCount fetches the rating count for a given user from the preloaded data
func getUserRatingCount(userId string) int {

	count, exists := userRatingCounts[userId]
	if !exists {
		return 0
	}
	return count
}

func Map(filename string, content string) []mr.KeyValue {
	kva := []mr.KeyValue{}
	// content: movieId userid,rating,userid,rating...\n
	// Split by \n
	lines := strings.Split(content, "\n")

	// Movie - user mapping
	movie_user := make(map[string]map[string]bool)
	// Co-rated matrix
	user_sim_matrix := make(map[string]map[string]int)

	// Iterate through each line (movie)
	for _, line := range lines {
		// Split line by space
		movieId_fields := strings.Split(line, " ")
		if len(movieId_fields) != 2 {
			continue
		}
		movieId := movieId_fields[0]

		// Ensure movie entry exists in movie_user
		if _, exists := movie_user[movieId]; !exists {
			movie_user[movieId] = make(map[string]bool)
		}

		// Get the ratings
		ratings := movieId_fields[1]
		rating_fields := strings.Split(ratings, ",")
		userRatings := make(map[string]float64)

		// Store every user's id and rating in a dictionary
		for i := 0; i < len(rating_fields); i += 2 {
			userId := rating_fields[i]
			rating, _ := strconv.ParseFloat(rating_fields[i+1], 64)
			userRatings[userId] = rating
			movie_user[movieId][userId] = true
		}

		// Calculate the co-rated matrix
		for u := range movie_user[movieId] {
			for v := range movie_user[movieId] {
				if u == v {
					continue
				}

				// Initialize if necessary
				if _, exists := user_sim_matrix[u]; !exists {
					user_sim_matrix[u] = make(map[string]int)
				}
				if _, exists := user_sim_matrix[v]; !exists {
					user_sim_matrix[v] = make(map[string]int)
				}

				// Increment the common rating count
				user_sim_matrix[u][v]++
				user_sim_matrix[v][u]++
			}
		}
	}
	//fmt.Println("Map: ", user_sim_matrix)
	// Emit the co-rated matrix as intermediate key-value pairs
	for u, relatedUsers := range user_sim_matrix {
		for v, count := range relatedUsers {
			kva = append(kva, mr.KeyValue{
				Key:   u + "," + v, // Users u and v are related
				Value: strconv.Itoa(count),
			})
		}
	}
	return kva
}

func Reduce(key string, values []string) string {
	// Key: "user1,user2"
	// Values: ["common_count1", "common_count2", ...]

	// Split key into user1 and user2
	users := strings.Split(key, ",")
	user1 := users[0]
	user2 := users[1]

	// Accumulate the total common rating count
	totalCommonCount := 0
	for _, val := range values {
		count, err := strconv.Atoi(val)
		if err != nil {
			continue // Skip invalid entries
		}
		totalCommonCount += count
	}
	// Simulate fetching user rating counts
	user1RatingCount := getUserRatingCount(user1) // Fetch the number of ratings user1 has given
	user2RatingCount := getUserRatingCount(user2) // Fetch the number of ratings user2 has given
	// Compute the similarity using cosine similarity
	if user1RatingCount == 0 || user2RatingCount == 0 {
		return ""
	}
	//NOTE - jaccard similarity
	similarity := float64(totalCommonCount) /
		(math.Sqrt(float64(user1RatingCount)) * math.Sqrt(float64(user2RatingCount)))

	// Emit the result: user pair and their similarity
	return fmt.Sprintf(",%.4f", similarity)

}
