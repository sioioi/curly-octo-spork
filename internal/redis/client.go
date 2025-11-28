package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type Client struct {
	db *goredis.Client
}

func NewClient(addr string) *Client {
	return &Client{
		db: goredis.NewClient(&goredis.Options{
			Addr: addr,
		}),
	}
}

func (c *Client) Close() error {
	return c.db.Close()
}

func (c *Client) SetOnline(ctx context.Context, sessionID string, ttl time.Duration) error {
	key := fmt.Sprintf("session:%s:online", sessionID)
	return c.db.Set(ctx, key, "1", ttl).Err()
}

func (c *Client) PushInbox(ctx context.Context, sessionID string, payload string) error {
	key := inboxKey(sessionID)
	return c.db.RPush(ctx, key, payload).Err()
}

func (c *Client) BlockPopInbox(ctx context.Context, sessionID string, timeout time.Duration) (string, error) {
	key := inboxKey(sessionID)
	values, err := c.db.BLPop(ctx, timeout, key).Result()
	if err == goredis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if len(values) != 2 {
		return "", nil
	}
	return values[1], nil
}

func (c *Client) SetSessionMode(ctx context.Context, sessionID, mode string, ttl time.Duration) error {
	key := fmt.Sprintf("session:%s:mode", sessionID)
	return c.db.Set(ctx, key, mode, ttl).Err()
}

func inboxKey(sessionID string) string {
	return fmt.Sprintf("session:%s:inbox", sessionID)
}
