package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"kafka-taskflow/internal/queue"
	"kafka-taskflow/internal/worker"
)

const brokerAddr = "localhost:9092"
const workerGroupID = "aircraft-workers"
const maxAttempts = 3

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// ctx cancels on Ctrl+C (SIGTERM) fro a clean shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	consumer := queue.NewConsumer(brokerAddr, queue.TopicTask, workerGroupID)
	defer consumer.Close()

	producer := queue.NewProducer(brokerAddr)
	defer producer.Close()

	w := worker.New(consumer, producer, maxAttempts, log)

	log.Info("worker starting")
	if err := w.Run(ctx); err != nil {
		log.Error("worker stopped with error", "err", err)
	}

	log.Info("worker stopped cleanly")
}
