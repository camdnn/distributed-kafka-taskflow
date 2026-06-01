// Package inject file used to inject an emergency task
package main

import (
	"kafka-taskflow/internal/queue"
	"kafka-taskflow/internal/task"

	"context"
	"log"
	"time"
)

// cmd/inject/main.go — throwaway
func main() {
	p := queue.NewProducer("localhost:9092")
	defer p.Close()

	sq := "7700"
	lat, lon := 33.9, -118.4
	a := &task.Aircraft{
		Hex: "test99", Flight: "TEST911",
		Lat: &lat, Lon: &lon, Squawk: &sq,
		ObservedAt: time.Now(),
	}
	payload, _ := a.ToJSON()
	t := task.NewTask("process_aircraft", payload)
	value, _ := t.ToJSON()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Publish(ctx, queue.TopicTask, []byte("test99"), value); err != nil {
		log.Fatal(err)
	}
	log.Println("injected emergency task")
}
