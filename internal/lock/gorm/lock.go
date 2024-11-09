package glock

import (
	"context"
	"errors"
	"github.com/ecodeclub/ekit/bean/option"
	"github.com/ecodeclub/ekit/retry"
	"github.com/google/uuid"
	"github.com/meoying/local-msg-go/internal/lock/errs"
	"gorm.io/gorm"
	"time"
)

const defaultTableName = "distributed_locks"

// Lock 基于 GORM 的实现，也就是基于关系型数据库的实现
// 整体思路就是借助乐观锁来控制状态，从而实现加锁和解锁的功能
// 它有两种模式，InsertFirst 和 UpdateFirst
type Lock struct {
	db         *gorm.DB
	key        string
	value      string
	valuer     func() string
	expiration time.Duration

	// 加锁的时候的单一一次超时
	lockTimeout time.Duration
	// 重试策略
	lockRetry retry.Strategy

	tableName string

	mode string
}

// NewLock 创建一个分布式锁，使用 ModeInsertFirst
func NewLock(db *gorm.DB,
	key string,
	expiration time.Duration,
	opts ...option.Option[Lock]) *Lock {
	strategy, _ := retry.NewExponentialBackoffRetryStrategy(time.Millisecond*100, time.Second, 10)
	l := &Lock{
		db: db,
		// 正常来说，访问 MySQL 虽然不是很快，但是也差不多就是十几二十毫秒的样子
		// 所以 500 ms 绰绰有余了
		lockTimeout: time.Millisecond * 500,
		valuer: func() string {
			return uuid.New().String()
		},
		key:        key,
		expiration: expiration,
		lockRetry:  strategy,
		tableName:  defaultTableName,
		mode:       ModeInsertFirst,
	}
	option.Apply(l, opts...)
	l.value = l.valuer()
	return l
}

func (l *Lock) Lock(ctx context.Context) error {
	switch l.mode {
	case ModeInsertFirst:
		return l.lockByInsertFirst(ctx)
	case ModeCASFirst:
		return l.lockByCASFirst(ctx)
	default:
		return errors.New("非法的锁模式")
	}

}

func (l *Lock) lockByInsertFirst(ctx context.Context) error {
	return retry.Retry(ctx, l.lockRetry, func() error {
		lctx, cancel := context.WithTimeout(ctx, l.lockTimeout)
		defer cancel()

		err := l.insertLock(lctx)
		// 加锁成功
		if err == nil {
			return nil
		}
		// 加锁失败有很多种可能，但是不管，我们默认是唯一索引冲突。
		// 因为我们并不知道用户用的是什么数据库，所以没办法检查是不是唯一索引冲突
		// 执行 CAS 操作
		return l.casLock(lctx)
	})
}

func (l *Lock) lockByCASFirst(ctx context.Context) error {
	return retry.Retry(ctx, l.lockRetry, func() error {
		lctx, cancel := context.WithTimeout(ctx, l.lockTimeout)
		defer cancel()

		// 执行 CAS 操作，CAS 操作失败，一般就是因为锁被人拿着
		err := l.casLock(lctx)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, gorm.ErrRecordNotFound):
			// 没有这条数据，说明没人加锁，尝试直接插入加锁
			return l.insertLock(lctx)
		default:
			// 不知道出了什么问题
			return err
		}
	})
}

// 使用 INSERT 来加锁，如果 insert 成功就说明拿到了锁
func (l *Lock) insertLock(ctx context.Context) error {
	now := time.Now().UnixMilli()
	db := l.db.WithContext(ctx)
	return db.Create(&DistributedLock{
		Key:        l.key,
		Value:      l.value,
		Status:     StatusLocked,
		Expiration: now + l.expiration.Milliseconds(),
		Version:    1,
		Utime:      now,
		Ctime:      now,
	}).Error
}

// 使用 CAS 机制来抢锁
func (l *Lock) casLock(ctx context.Context) error {
	db := l.db.WithContext(ctx)
	var lock DistributedLock
	err := db.Where(&DistributedLock{Key: l.key}).First(&lock).Error
	if err != nil {
		// 查询失败，可能
		// 1. 数据库中没有数据
		// 2. 查询本身有问题
		return err
	}
	now := time.Now().UnixMilli()
	if lock.Status == StatusLocked && lock.Value == l.value {
		// 自己之前加锁成功了。比如说因为超时之类的导致第一次加锁成功了但是没收到成功响应
		// 那么重试的时候，就会直接成功
		return nil
	}
	// 还在被人拿着
	if lock.Status == StatusLocked &&
		now < lock.Expiration {
		return errs.ErrLocked
	}
	// 到这里有两种可能，
	// 1. Status 是 Unlocked
	// 2. Status 是 Locked 但是因为没有续期，相当于之前的节点已经放弃锁了
	// 不建议用 utime 作为版本号，因为高并发的环境下，可能有两个毫秒数相同的
	res := db.Model(&DistributedLock{}).Where(&DistributedLock{Key: l.key, Version: lock.Version}).
		Updates(map[string]interface{}{
			"status":     StatusLocked,
			"utime":      now,
			"value":      l.value,
			"expiration": now + l.expiration.Milliseconds(),
			"version":    lock.Version + 1,
		})
	if res.Error != nil {
		// 数据库有问题
		return res.Error
	}
	// 加锁成功
	if res.RowsAffected > 0 {
		return nil
	}
	// 刚刚被人抢走
	return errs.ErrLocked
}

func (l *Lock) Unlock(ctx context.Context) error {
	now := time.Now().UnixMilli()
	res := l.db.WithContext(ctx).Model(&DistributedLock{}).Where(&DistributedLock{
		Key:   l.key,
		Value: l.value,
	}).Updates(map[string]interface{}{
		"utime":  now,
		"status": StatusUnlocked,
		// 将过期时间修改当下。当然不修改也是可以的
		"expiration": now,
	})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected > 0 {
		return nil
	}
	return errs.ErrLockNotHold
}

func (l *Lock) Refresh(ctx context.Context) error {
	now := time.Now().UnixMilli()
	res := l.db.WithContext(ctx).Model(&DistributedLock{}).
		// 要确保还没有过期
		Where("`key` = ? AND `value`= ? AND `status` = ? AND expiration > ?", l.key, l.value, StatusLocked, now).
		Updates(map[string]interface{}{
			"utime":      now,
			"expiration": now + l.expiration.Milliseconds(),
		})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected > 0 {
		return nil
	}
	return errs.ErrLockNotHold
}

// DistributedLock 在数据库中保存的代表锁的东西
// 注意这里我们不需要一个 expiration 字段
type DistributedLock struct {
	Id int64 `gorm:"primaryKey,autoIncrement"`
	// 唯一索引
	Key string `gorm:"unique,type=VARCHAR(256)"`
	// 用固定长度的 CHAR 稍微有点性能提升
	Value string `gorm:"type=CHAR(64)"`

	Status uint8

	// int64 够你用到天荒地老
	Version int64

	// 过期时间，毫秒数
	Expiration int64 `gorm:"index"`

	// 记录的是毫秒数
	Utime int64 `gorm:"index"`
	Ctime int64
}

const (
	StatusUnlocked uint8 = iota
	StatusLocked
)

func (l DistributedLock) TableName() string {
	return defaultTableName
}
