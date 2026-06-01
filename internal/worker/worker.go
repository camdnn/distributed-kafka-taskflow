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

// Run contains workflow to fetch the data, handle, and commit when successful
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
		if err := w.consumer.Commit(ctx, msg); err != nil {
			w.log.Error("commit failed", "err", err)
		}
	}
}

// handle perfoms deocdes the data and validates data and detects emergencies
func (w *Worker) handle(ctx context.Context, value []byte) {
	t, err := task.FromJSON(value)
	if err != nil {
		w.log.Error("undecodeable task, skipping", "err", err)
		return
	}

	// start running processes on the task
	w.publishStatus(ctx, t, task.StatusRunning, "")

	// decode payload into aircraft struct
	a, err := task.AircraftFromJSON(t.Payload)
	if err != nil {
		w.deadLetter(ctx, t, a, err.Error())
		return
	}

	// create a new key for idempotency
	key := a.Hex + a.ObservedAt.Format("2006-01-02T15:04:05.000000000Z07:00")
	if _, dup := w.seen.Load(key); dup {
		w.log.Info("duplicate, skipping", "hex", a.Hex)
		return
	}

	// if airplane data is invalidated handle the error
	if err := a.Validate(); err != nil {
		w.handleFailure(ctx, t, a, err.Error())
		return
	}

	// if a squawk emergency code detected then create a new alert and publish it
	if a.IsEmergency() {
		w.publish(ctx, queue.TopicAlerts, task.NewAlert(a))
	}

	// public the passed aircraft data to result topic
	w.publish(ctx, queue.TopicResults, task.NewResult(a))
	w.log.Info("processed aircraft", "hex", a.Hex, "flight", a.Flight)
	w.publishStatus(ctx, t, task.StatusCompleted, "")
	w.seen.Store(key, struct{}{})

}

// routes a validation failure to retires or dead-letter
func (w *Worker) handleFailure(ctx context.Context, t *task.Task, a *task.Aircraft, reason string) {
	if t.Attempts >= w.maxAttempts {
		w.deadLetter(ctx, t, a, reason)
	} else {
		w.publish(ctx, queue.TopicRetries, t)
		w.publishStatus(ctx, t, task.StatusRetrying, reason)
	}
}

// publish a dead letter topic
func (w *Worker) deadLetter(ctx context.Context, t *task.Task, a *task.Aircraft, reason string) {
	hex := ""
	if a != nil {
		hex = a.Hex
	}

	// create new deadletter and publish
	dl := task.NewDeadLetter(t.ID, hex, reason, t.Attempts)

	w.publish(ctx, queue.TopicDeadLetter, dl)
	w.publishStatus(ctx, t, task.StatusDeadLettered, reason)
}

// send the task to the specified topic
func (w *Worker) publish(ctx context.Context, topic string, event interface{ ToJSON() ([]byte, error) }) {

	// convert event to json
	data, err := event.ToJSON()
	if err != nil {
		w.log.Error("serialize event failed", "topic", topic, "err", err)
		return
	}

	// publish event to specific toppic
	if err := w.producer.Publish(ctx, topic, nil, data); err != nil {
		w.log.Error("publish failed", "topic", topic, "err", err)
	}

}

func (w *Worker) publishStatus(ctx context.Context, t *task.Task, status task.TaskStatus, msg string) {
	ev := task.NewStatusEvent(t, status, msg)
	w.publish(ctx, queue.TopicStatus, ev)
}
