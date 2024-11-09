package glock

import (
	"context"
	"github.com/meoying/local-msg-go/internal/lock/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"testing"
	"time"
)

type LockTestSuite struct {
	suite.Suite
	db *gorm.DB
}

func (s *LockTestSuite) SetupSuite() {
	db, err := gorm.Open(mysql.Open("root:root@tcp(localhost:13316)/local_msg_test?charset=utf8mb4&collation=utf8mb4_general_ci&parseTime=True&loc=Local&timeout=1s&readTimeout=3s&writeTimeout=3s"))
	require.NoError(s.T(), err)
	s.db = db
	err = s.db.AutoMigrate(&DistributedLock{})
	require.NoError(s.T(), err)
}

func (s *LockTestSuite) TearDownTest() {
	err := s.db.Exec("TRUNCATE TABLE distributed_locks").Error
	require.NoError(s.T(), err)
}

func (s *LockTestSuite) TestLock() {
	testCases := []struct {
		name       string
		key        string
		expiration time.Duration
		before     func(t *testing.T)
		after      func(t *testing.T)

		wantErr error
	}{
		{
			name:       "直接加锁成功",
			key:        "success_key_1",
			expiration: time.Minute,
			before: func(t *testing.T) {

			},
			after: func(t *testing.T) {
				// 验证数据库中的数据
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				var lock DistributedLock
				err := s.db.WithContext(ctx).Where("`key` = ?", "success_key_1").
					First(&lock).Error
				assert.NoError(s.T(), err)
				assert.True(t, lock.Utime > 0)
				assert.True(t, lock.Ctime > 0)
				assert.Equal(t, "success_key_1", lock.Key)
				assert.True(t, len(lock.Value) > 0)
				assert.Equal(t, StatusLocked, lock.Status)
				assert.True(s.T(), lock.Expiration > time.Now().UnixMilli())
			},
		},
		{
			name:       "cas 加锁成功",
			key:        "success_key_2",
			expiration: time.Minute,
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				err := s.db.WithContext(ctx).Create(&DistributedLock{
					Key:     "success_key_2",
					Utime:   123,
					Ctime:   123,
					Status:  StatusUnlocked,
					Value:   "abc",
					Version: 12,
				}).Error
				require.NoError(s.T(), err)
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				var lock DistributedLock
				err := s.db.WithContext(ctx).Where("`key` = ?", "success_key_2").First(&lock).Error
				assert.NoError(s.T(), err)
				assert.Equal(s.T(), StatusLocked, lock.Status)
				assert.True(s.T(), lock.Utime > 123)
				assert.Equal(s.T(), int64(123), lock.Ctime)
				assert.Equal(s.T(), int64(13), lock.Version)
				assert.True(t, lock.Expiration > time.Now().UnixMilli())
			},
		},
		{
			name:       "加锁成功，原持有人崩溃",
			key:        "success_key_3",
			expiration: time.Minute,
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				err := s.db.WithContext(ctx).Create(&DistributedLock{
					Key:        "success_key_3",
					Utime:      123,
					Ctime:      123,
					Status:     StatusLocked,
					Expiration: time.Now().UnixMilli() - 10,
					Value:      "abc",
					Version:    12,
				}).Error
				require.NoError(s.T(), err)
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				var lock DistributedLock
				err := s.db.WithContext(ctx).Where("`key` = ?", "success_key_3").First(&lock).Error
				assert.NoError(s.T(), err)
				assert.Equal(s.T(), StatusLocked, lock.Status)
				assert.True(s.T(), lock.Utime > 123)
				assert.Equal(s.T(), int64(123), lock.Ctime)
				assert.Equal(s.T(), int64(13), lock.Version)
				assert.True(s.T(), lock.Expiration > time.Now().UnixMilli())
			},
		},
		{
			name:       "加锁失败，锁正在被人持有",
			key:        "fail_key_1",
			expiration: time.Minute,
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				now := time.Now().UnixMilli()
				err := s.db.WithContext(ctx).Create(&DistributedLock{
					Key:        "fail_key_1",
					Utime:      now,
					Ctime:      now,
					Status:     StatusLocked,
					Expiration: time.Now().Add(time.Minute).UnixMilli(),
					Value:      "abc",
					Version:    12,
				}).Error
				require.NoError(s.T(), err)
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				var lock DistributedLock
				err := s.db.WithContext(ctx).Where("`key` = ?", "fail_key_1").First(&lock).Error
				assert.NoError(s.T(), err)
				assert.Equal(s.T(), StatusLocked, lock.Status)
				assert.Equal(s.T(), int64(12), lock.Version)
			},
			wantErr: errs.ErrLocked,
		},
	}

	// 两种模式下，测试结果应该都是一样的
	s.T().Run(ModeInsertFirst, func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				lock := NewLock(s.db, tc.key, tc.expiration, WithMode(ModeCASFirst))
				tc.before(t)
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				err := lock.Lock(ctx)
				assert.ErrorIs(t, err, tc.wantErr)
				tc.after(t)
			})
		}
	})

	s.TearDownTest()

	s.T().Run(ModeCASFirst, func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				lock := NewLock(s.db, tc.key, tc.expiration, WithMode(ModeCASFirst))
				tc.before(t)
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				err := lock.Lock(ctx)
				assert.ErrorIs(t, err, tc.wantErr)
				tc.after(t)
			})
		}
	})
}

func (s *LockTestSuite) TestUnlock() {
	testCases := []struct {
		name   string
		before func(t *testing.T) *Lock
		after  func(t *testing.T)

		wantErr error
	}{
		{
			name: "解锁成功",
			before: func(t *testing.T) *Lock {
				// 模拟加锁成功
				lock := NewLock(s.db, "unlock_key1", time.Minute)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				err := lock.Lock(ctx)
				require.NoError(t, err)
				return lock
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				var lock DistributedLock
				err := s.db.WithContext(ctx).Where("`key` = ?", "unlock_key1").First(&lock).Error
				require.NoError(t, err)
				assert.Equal(s.T(), StatusUnlocked, lock.Status)
			},
		},

		{
			name: "解锁失败-值不匹配",
			before: func(t *testing.T) *Lock {
				// 模拟加锁成功
				lock := NewLock(s.db, "unlock_key2", time.Second)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				err := lock.Lock(ctx)
				require.NoError(t, err)
				// 篡改了值
				lock.value = "123"
				return lock
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				var lock DistributedLock
				err := s.db.WithContext(ctx).Where("`key` = ?", "unlock_key2").First(&lock).Error
				require.NoError(t, err)
				assert.Equal(s.T(), StatusLocked, lock.Status)
			},
			wantErr: errs.ErrLockNotHold,
		},

		{
			name: "解锁失败-锁不存在",
			before: func(t *testing.T) *Lock {
				// 模拟加锁成功
				lock := NewLock(s.db, "unlock_key3", time.Second)
				return lock
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				var lock DistributedLock
				err := s.db.WithContext(ctx).Where("`key` = ?", "unlock_key3").First(&lock).Error
				assert.ErrorIs(t, err, gorm.ErrRecordNotFound)
			},
			wantErr: errs.ErrLockNotHold,
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			lock := tc.before(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			err := lock.Unlock(ctx)
			assert.ErrorIs(t, err, tc.wantErr)
			tc.after(t)
		})
	}
}

func (s *LockTestSuite) TestRefresh() {
	testCases := []struct {
		name string

		before func(t *testing.T) *Lock
		after  func(t *testing.T)

		wantErr error
	}{
		{
			name: "刷新成功",
			before: func(t *testing.T) *Lock {
				lock := NewLock(s.db, "refresh_key1", time.Second*10)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				err := lock.Lock(ctx)
				require.NoError(t, err)
				return lock
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				var lock DistributedLock
				err := s.db.WithContext(ctx).Where("`key` = ?", "refresh_key1").First(&lock).Error
				require.NoError(t, err)
				assert.Equal(s.T(), StatusLocked, lock.Status)
				assert.True(s.T(), lock.Expiration > time.Now().UnixMilli())
			},
		},

		{
			name: "刷新失败，已经过期了",
			before: func(t *testing.T) *Lock {

				lock := NewLock(s.db, "refresh_key2", time.Second*10)
				lock.value = "123"
				now := time.Now().UnixMilli()
				err := s.db.Create(&DistributedLock{
					Key:        "refresh_key2",
					Expiration: now - 100,
					Utime:      now,
					Ctime:      now,
					Status:     StatusLocked,
					Value:      "123",
				}).Error
				require.NoError(t, err)
				return lock
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				var lock DistributedLock
				err := s.db.WithContext(ctx).Where("`key` = ?", "refresh_key2").First(&lock).Error
				require.NoError(t, err)
				// 什么也没变
				assert.Equal(s.T(), StatusLocked, lock.Status)
				assert.True(s.T(), lock.Expiration < time.Now().UnixMilli())
			},
			wantErr: errs.ErrLockNotHold,
		},

		{
			name: "刷新失败，状态不对",
			before: func(t *testing.T) *Lock {

				lock := NewLock(s.db, "refresh_key3", time.Second*10)
				lock.value = "123"
				now := time.Now().UnixMilli()
				err := s.db.Create(&DistributedLock{
					Key:        "refresh_key3",
					Expiration: now + time.Minute.Milliseconds(),
					Utime:      now,
					Ctime:      now,
					Status:     StatusUnlocked,
					Value:      "123",
				}).Error
				require.NoError(t, err)
				return lock
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				var lock DistributedLock
				err := s.db.WithContext(ctx).Where("`key` = ?", "refresh_key3").First(&lock).Error
				require.NoError(t, err)
				// 什么也没变
				assert.Equal(s.T(), StatusUnlocked, lock.Status)
			},
			wantErr: errs.ErrLockNotHold,
		},

		{
			// 也就是别人的锁
			name: "刷新失败，值不对",
			before: func(t *testing.T) *Lock {

				lock := NewLock(s.db, "refresh_key4", time.Second*10)
				lock.value = "123"
				now := time.Now().UnixMilli()
				err := s.db.Create(&DistributedLock{
					Key:        "refresh_key4",
					Expiration: now + time.Minute.Milliseconds(),
					Utime:      now,
					Ctime:      now,
					Status:     StatusLocked,
					Value:      "234",
				}).Error
				require.NoError(t, err)
				return lock
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				var lock DistributedLock
				err := s.db.WithContext(ctx).Where("`key` = ?", "refresh_key4").First(&lock).Error
				require.NoError(t, err)
				// 什么也没变
				assert.Equal(s.T(), StatusLocked, lock.Status)
			},
			wantErr: errs.ErrLockNotHold,
		},
	}
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			lock := tc.before(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			err := lock.Refresh(ctx)
			assert.ErrorIs(t, err, tc.wantErr)
			tc.after(t)
		})
	}
}

func TestLockTestSuite(t *testing.T) {
	suite.Run(t, new(LockTestSuite))
}

// 1. chatgpt 整理知识点，强调一下在面试中深入、透彻分析问题，结合实际案例，赢得竞争优势
// 2. 你看看知识点是否有遗漏、重复、衍生。
// 3. 再次要求 chatgpt 输出人类回答的话，禁用列表就可以
