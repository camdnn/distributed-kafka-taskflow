// Package retry handeles retries
package retry

import (
	"context"
	"kafka-taskflow/internal/queue"
	"kafka-taskflow/internal/task"
	"log/slog"
	"time"
)

type Handler struct {
	consumer *queue.Consumer
	producer *queue.Producer
	log      *slog.Logger
}

func New(c *queue.Consumer, p *queue.Producer, log *slog.Logger) *Handler {
	return &Handler{
		consumer: c,
		producer: p,
		log:      log,
	}
}

func (h *Handler) Run(ctx context.Context) error {
	for {
		msg, err := h.consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			h.log.Error("fetch failed", "err", err)
			continue
		}

		t, err := task.FromJSON(msg.Value)

		// if cannot decode commit to move the offset
		if err != nil {
			h.log.Error("undecodeable task", "err", err)
			if err := h.consumer.Commit(ctx, msg); err != nil {
				h.log.Error("unable to commit undecodeable task", "err", err)
			}
			continue
		}

		if t.RetryAt != nil {
			if wait := time.Until(*t.RetryAt); wait > 0 {
				select {
				case <-time.After(wait): // do nothing so we can go down to tasks below
				case <-ctx.Done():
					return nil
				}
			}
		}

		t.Attempts++
		t.RetryAt = nil

		value, err := t.ToJSON()
		if err != nil {
			h.log.Error("serialize failed", "err", err)
			if err := h.consumer.Commit(ctx, msg); err != nil {
				h.log.Error("unable to commit a failed serialized task", "err", err)
			}
			continue
		}

		// try to pick a stable key (prefer payload.hex when present)
		key := []byte(t.ID)
		if a, err := task.AircraftFromJSON(t.Payload); err == nil && a.Hex != "" {
			key = []byte(a.Hex)
		}

		// publish the task to the task topic
		if err := h.producer.Publish(ctx, queue.TopicTask, key, value); err != nil {
			h.log.Error("republish failed", "err", err)
			continue
		}

		// commit the task to increment the offset
		if err := h.consumer.Commit(ctx, msg); err != nil {
			h.log.Error("unable to commit to requeue task", "err", err)
		}
		h.log.Info("requeued task", "id", t.ID, "attempt", t.Attempts)

	}
}

// Backoff calculate the backoff per attempts
// use a bitwise shift to calc powers of 2
func Backoff(attempts int) time.Duration {
	const maxBackoff = 60 * time.Second
	if attempts >= 6 {
		return maxBackoff
	}
	return min(time.Second<<attempts, maxBackoff)
}
