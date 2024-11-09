package glock

import (
	"context"
	dlock "github.com/meoying/local-msg-go/internal/lock"
	"gorm.io/gorm"
	"time"
)

type Client struct {
	db *gorm.DB
}

func NewClient(db *gorm.DB) *Client {
	return &Client{db: db}
}

func (c *Client) NewLock(ctx context.Context, key string, expiration time.Duration) (dlock.Lock, error) {
	return NewLock(c.db, key, expiration), nil
}

func (c *Client) InitTable() error {
	return c.db.AutoMigrate(&DistributedLock{})
}
