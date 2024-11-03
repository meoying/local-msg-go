package rlock

import (
	"context"
	_ "embed"
	"errors"
	"github.com/ecodeclub/ekit/bean/option"
	"github.com/ecodeclub/ekit/retry"
	"github.com/google/uuid"
	"github.com/meoying/local-msg-go/internal/lock/errs"
	"github.com/redis/go-redis/v9"
	"time"
)

var (
	//go:embed lua/unlock.lua
	luaUnlock string
	//go:embed lua/refresh.lua
	luaRefresh string

	//go:embed lua/lock.lua
	luaLock string
)

// Lock 这是一个不可重入锁，因为可重入本质上是一个垃圾代码引发的需求。
// 你需要先通过 NewLock 来创建一个实例，而后尝试修改其中的部分参数
type Lock struct {
	client     redis.Cmdable
	key        string
	value      string
	valuer     func() string
	expiration time.Duration

	// 加锁的时候的单一一次超时
	lockTimeout time.Duration
	// 重试策略
	lockRetry retry.Strategy
}

// NewLock 创建一个分布式锁
// rdb 是 Redis 客户端
// 默认情况下采用指数退避的算法来重试。
func NewLock(rdb redis.Cmdable, key string,
	expiration time.Duration, opts ...option.Option[Lock]) *Lock {
	strategy, _ := retry.NewExponentialBackoffRetryStrategy(time.Millisecond*100, time.Second, 10)
	l := &Lock{
		client: rdb,
		// 正常来说，访问 Redis 是一个快的事情，所以 200ms 是绰绰有余的
		// 毕竟一般来说超过 10ms 就是 Redis 上的慢查询了
		lockTimeout: time.Millisecond * 200,
		valuer: func() string {
			return uuid.New().String()
		},
		key:        key,
		expiration: expiration,
		lockRetry:  strategy,
	}
	option.Apply(l, opts...)
	l.value = l.valuer()
	return l
}

// Lock 会尝试加锁。当加锁失败的时候，会尝试重试。
// lockTimeout 参数会在这里用来控制访问 Redis 的超时时间
func (l *Lock) Lock(ctx context.Context) error {
	return retry.Retry(ctx, l.lockRetry, func() error {
		lctx, cancel := context.WithTimeout(ctx, l.lockTimeout)
		defer cancel()
		res, err := l.client.Eval(lctx,
			luaLock,
			[]string{l.key}, l.value, l.expiration).Result()
		// 加锁失败。虽然从理论上来说，此时加锁有可能是因为一些不可挽回的错误造成的
		// 但是我们这里没有区分处理
		if err != nil {
			return err
		}
		if res == "OK" {
			return nil
		}
		return errs.ErrLocked
	})
}

// Refresh 延长过期时间。
func (l *Lock) Refresh(ctx context.Context) error {
	res, err := l.client.Eval(ctx, luaRefresh,
		[]string{l.key}, l.value, l.expiration.Seconds()).Int64()
	if err != nil {
		return err
	}
	if res != 1 {
		return errs.ErrLockNotHold
	}
	return nil
}

// Unlock 并不会重试。如果失败了那就是失败了，等它自然过期
func (l *Lock) Unlock(ctx context.Context) error {
	res, err := l.client.Eval(ctx, luaUnlock, []string{l.key}, l.value).Int64()
	if errors.Is(err, redis.Nil) {
		return errs.ErrLockNotHold
	}
	if err != nil {
		return err
	}
	if res != 1 {
		return errs.ErrLockNotHold
	}
	return nil
}
