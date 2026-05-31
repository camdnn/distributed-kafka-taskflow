package queue

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// Producer wraps a kafka-go writer behind a small interface so the rest of
// the system produces without knowing kafka-go exists:
type Producer struct {
	writer *kafka.Writer
}

// NewProducer builds a Producer aimed at a broker. The writer is configured
// once here so every Publish uses same durability guarantees.
// We use require all since publish contains a timeout
func NewProducer(brokerAddr string) *Producer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokerAddr),
		RequiredAcks: kafka.RequireAll,
		Balancer:     &kafka.Hash{}, // hash allows us to use key to partition work
		Async:        false,         // blocks until broker acks or context expires
	}
	return &Producer{writer: w}
}

// Publish sends one message. Key (hex code for -> same partition -> same worker).
// value is the already-serialized payload (Task.ToJSON()).
// context.Context contains signal for when to stop [timeout]
func (p *Producer) Publish(ctx context.Context, topic string, key, value []byte) error {
	err := p.writer.WriteMessages(ctx, kafka.Message{Topic: topic, Key: key, Value: value})
	if err != nil {
		return fmt.Errorf("unable for producer to publish mesage %w", err)
	}

	return nil
}

// Close flushes and releases the connection.
func (p *Producer) Close() error {
	return p.writer.Close()
}
