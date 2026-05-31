package main

import (
	"context"
	"log"
	"time"

	"kafka-taskflow/internal/queue"
	"kafka-taskflow/internal/task"
)

func main() {
	p := queue.NewProducer("localhost:9092")
	defer p.Close()

	t := task.NewTask("process_aircraft", []byte(`{"hex":"test1","squawk":"1200"}`))
	value, err := t.ToJSON()
	if err != nil {
		log.Fatalf("serialize: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := p.Publish(ctx, "tasks", []byte("test1"), value); err != nil {
		log.Fatalf("publish: %v", err)
	}
	log.Println("published OK")
}
