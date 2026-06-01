// Package worker
package worker

import (
	"context"
	"log/slog"
	"sync"

	"kafka-taskflow/internal/queue"
	"kafka-taskflow/internal/task"
)

type Worker struct {
	consumer    *queue.Consumer
	producer    *queue.Producer
	maxAttempts int
	seen        sync.Map
	log         *slog.Logger
}

// New is a ctor to build a new worker
func New(c *queue.Consumer, p *queue.Producer, maxAttempts int, log *slog.Logger) *Worker {
	return &Worker{
		consumer:    c,
		producer:    p,
		maxAttempts: maxAttempts,
		log:         log,
	}
}

// Run contains a ACID workflow to fetch the data, handle, and commit when successful
func (w *Worker) Run(ctx context.Context) error {
	for {
		msg, err := w.consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.log.Error("fetch failed", "err", err)
			continue
		}

		w.handle(ctx, msg.Value)

	}
}

func (w *Worker) handle(ctx context.Context, value []byte) {
	t, err := task.FromJSON(value)
	if err != nil {
		w.log.Error("undecodeable task, skipping", "err", err)
		return
	}

	a, err := task.AircraftFromJSON(t.Payload)
	if err != nil {
		w.deadLetter(ctx, t, a, err.Error())
	}

}

func (w *Worker) handleFailure(ctx context.Context, t *task.Task, a *task.Aircraft, reason string) {

}

// publish a dead letter topic
func (w *Worker) deadLetter(ctx context.Context, t *task.Task, a *task.Aircraft, reason string) {
	hex := ""
	if a != nil {
		hex = a.Hex
	}
	dl := task.NewDeadLetter(t.ID, hex, reason, t.Attempt)
	w.publish(ctx, queue.TopicDeadLetter, dl)
	w.publishStatus(ctx, t, task.StatusDeadLettered, reason)
}

// send the task to the specified topic
func (w *Worker) publish(ctx context.Context, topic string, event interface{ ToJSON() ([]byte, error) }) {
	data, err := event.ToJSON()
	if err != nil {
		w.log.Error("serialize event failed", "topic", topic, "err", err)
		return
	}

	if err := w.producer.Publish(ctx, topic, nil, data); err != nil {
		w.log.Error("publish failed", "topic", topic, "err", err)
	}

}

// publish to the status topic
func (w *Worker) publishStatus(ctx context.Context, t *task.Task, status task.TaskStatus, msg string) {
	ev := task.NewStatusEvent(t, status, msg)
	w.publish(ctx, queue.TopicStatus, ev)
}
