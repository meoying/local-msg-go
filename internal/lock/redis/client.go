package rlock

import (
	"context"
	dlock "github.com/meoying/local-msg-go/internal/lock"
	"github.com/redis/go-redis/v9"
	"time"
)

type Client struct {
	rdb redis.Cmdable
}

func NewClient(rdb redis.Cmdable) *Client {
	return &Client{rdb: rdb}
}

func (c *Client) NewLock(ctx context.Context, key string, expiration time.Duration) (dlock.Lock, error) {
	return NewLock(c.rdb, key, expiration), nil
}
