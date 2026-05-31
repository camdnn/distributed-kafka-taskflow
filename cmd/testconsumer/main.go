package main

import (
	"context"
	"log"
	"time"

	"kafka-taskflow/internal/queue"
)

func main() {
	c := queue.NewConsumer("localhost:9092", "tasks", "test-group")
	defer c.Close()

	for {
		msg, err := c.Fetch(context.Background())
		if err != nil {
			log.Fatalf("fetch: %v", err)
		}
		log.Printf("PROCESSING: %s", msg.Value)
		time.Sleep(10 * time.Second) // simulate work — the kill window
		log.Printf("DONE, committing: %s", msg.Value)
		if err := c.Commit(context.Background(), msg); err != nil {
			log.Fatalf("commit: %v", err)
		}
	}
}
