package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"kafka-taskflow/internal/queue"
	"kafka-taskflow/internal/retry"
)

const brokerAddr = "localhost:9092"
const workerGroupID = "retry-handler"
const maxAttempts = 3

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// ctx cancels on Ctrl+C (SIGTERM) fro a clean shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	consumer := queue.NewConsumer(brokerAddr, queue.TopicRetries, workerGroupID)
	defer consumer.Close()

	producer := queue.NewProducer(brokerAddr)
	defer producer.Close()

	r := retry.New(consumer, producer, log)

	log.Info("retry-handler starting")
	if err := r.Run(ctx); err != nil {
		log.Error("retry-handler stopped with error", "err", err)
	}

	log.Info("worker stopped cleanly")
}
