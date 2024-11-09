package dlock

import (
	"context"
	"time"
)

type Client interface {
	// NewLock 初始化一个锁
	NewLock(ctx context.Context, key string, expiration time.Duration) (Lock, error)
}

type Lock interface {
	Lock(ctx context.Context) error
	Unlock(ctx context.Context) error

	// Refresh 续约，延长过期时间
	Refresh(ctx context.Context) error
}
