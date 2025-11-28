package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Record is a minimal interface exposed to downstream consumers.
type Record interface {
	Key() []byte
	Value() []byte
}

type consumerRecord struct {
	record *kgo.Record
}

func (r consumerRecord) Key() []byte   { return r.record.Key }
func (r consumerRecord) Value() []byte { return r.record.Value }

// Consumer wraps a franz-go client and exposes a callback-based loop.
type Consumer struct {
	client *kgo.Client
}

func NewConsumer(brokers []string, groupID string, topics []string) (*Consumer, error) {
	if len(topics) == 0 {
		return nil, fmt.Errorf("topics required")
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topics...),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("new client: %w", err)
	}

	return &Consumer{client: client}, nil
}

func (c *Consumer) Consume(ctx context.Context, fn func(Record) error) error {
	for {
		fetches := c.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, ferr := range errs {
				return fmt.Errorf("fetch err: %w", ferr.Err)
			}
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			if err := fn(consumerRecord{record: rec}); err != nil {
				return err
			}
		}
	}
}

func (c *Consumer) Close() {
	c.client.Close()
}
