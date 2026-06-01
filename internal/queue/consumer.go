// Package queue holds all queue tasks
package queue

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// Consumer wraps a kafka-go Reader in a consumer group.
type Consumer struct {
	reader *kafka.Reader
}

// NewConsumer constructor to make a new consumer
// need the brokerID to connect to kafka
// need topic to know what topic to read from
// need groupID for the parition on topic -> form a group taht kafka can assign disjoing partitions to
func NewConsumer(brokerAddr, topic, groupID string) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{brokerAddr},
		Topic:   topic,
		GroupID: groupID})

	return &Consumer{reader: r}

}

// Fetch Read the message from the topic without advancing the offset
// send out the message
func (c *Consumer) Fetch(ctx context.Context) (*kafka.Message, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to fetch message %+v: %w", ctx, err)

	}
	return &msg, nil
}

// Commit commits message advancing the offset
// is called only after the work is handeled
func (c *Consumer) Commit(ctx context.Context, msg *kafka.Message) error {
	err := c.reader.CommitMessages(ctx, *msg)
	if err != nil {
		return fmt.Errorf("unable to commit message %w", err)
	}

	return nil
}

// Close consumer
func (c *Consumer) Close() error {
	return c.reader.Close()
}
