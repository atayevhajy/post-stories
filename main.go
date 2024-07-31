package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
)

var rdb *redis.Client

func init() {
	rdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	_, err := rdb.Ping(context.Background()).Result()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to Redis!")
}

func AddFollowers(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	followerKey := fmt.Sprintf("user:followers:%s", userID)
	for i := 1; i <= 10000; i++ {
		followerID := strconv.Itoa(i)
		_, err := rdb.SAdd(context.Background(), followerKey, followerID).Result()
		if err != nil {
			http.Error(w, "Error adding follower", http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "10,000 followers added successfully")
}

func PostStories(w http.ResponseWriter, r *http.Request) {
	storyCount := 20
	userID := r.URL.Query().Get("user_id")
	storyCountStr := r.URL.Query().Get("story_count")
	storyCount, err := strconv.Atoi(storyCountStr)
	if err != nil || storyCount <= 0 {
		http.Error(w, "Invalid story_count", http.StatusBadRequest)
		return
	}

	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	for i := 1; i <= storyCount; i++ {
		storyContent := fmt.Sprintf("Story %d content", i)
		err := postStoryToFollowers(userID, storyContent)
		if err != nil {
			http.Error(w, "Error posting stories", http.StatusInternalServerError)
			return
		}
		fmt.Printf("Story %d content\n", i)
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "%d stories posted successfully to 10,000 followers", storyCount)
}

func postStoryToFollowers(userID, storyContent string) error {
	followerKey := fmt.Sprintf("user:followers:%s", userID)
	notificationTemplate := fmt.Sprintf("New story from %s: %s", userID, storyContent)

	script := `
    local followers = redis.call("SMEMBERS", KEYS[1])
    for i=1,#followers do
        local notificationKey = "user:notifications:" .. followers[i]
        redis.call("LPUSH", notificationKey, ARGV[1])
    end
    return #followers
    `
	fmt.Printf("New story from %s: %s", userID, storyContent)

	_, err := rdb.Eval(context.Background(), script, []string{followerKey}, notificationTemplate).Result()
	return err
}

func main() {
	r := mux.NewRouter()

	r.HandleFunc("/add-followers", AddFollowers).Methods("POST")
	r.HandleFunc("/post-stories", PostStories).Methods("POST")

	http.Handle("/", r)
	fmt.Println("Server is running on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
