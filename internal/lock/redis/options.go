package rlock

import (
	"github.com/ecodeclub/ekit/bean/option"
	"github.com/ecodeclub/ekit/retry"
	"time"
)

// WithLockTimeout 指定单一一次加锁的超时时间
func WithLockTimeout(timeout time.Duration) option.Option[Lock] {
	return func(t *Lock) {
		t.lockTimeout = timeout
	}
}

func WithLockRetryStrategy(strategy retry.Strategy) option.Option[Lock] {
	return func(t *Lock) {
		t.lockRetry = strategy
	}
}
