package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer wraps a franz-go client for simple fire-and-forget writes.
type Producer struct {
	client *kgo.Client
	topic  string
}

func NewProducer(brokers []string, topic string) (*Producer, error) {
	if topic == "" {
		return nil, fmt.Errorf("topic is required")
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("new client: %w", err)
	}

	return &Producer{client: client, topic: topic}, nil
}

func (p *Producer) Produce(ctx context.Context, key string, value []byte) error {
	record := &kgo.Record{
		Topic: p.topic,
		Value: value,
	}
	if key != "" {
		record.Key = []byte(key)
	}

	return p.client.ProduceSync(ctx, record).FirstErr()
}

func (p *Producer) Close() {
	p.client.Close()
}
