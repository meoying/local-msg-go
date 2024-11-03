// Copyright 2021 gotomicro
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rlock

import (
	"context"
	"errors"
	"github.com/ecodeclub/ekit/retry"
	"github.com/meoying/local-msg-go/internal/lock/errs"
	"github.com/redis/go-redis/v9"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ClientE2ESuite struct {
	suite.Suite
	rdb redis.Cmdable
}

func (s *ClientE2ESuite) SetupSuite() {
	s.rdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	// 确保测试的目标 Redis 已经启动成功了
	for s.rdb.Ping(context.Background()).Err() != nil {

	}
}

func TestClientE2E(t *testing.T) {
	suite.Run(t, &ClientE2ESuite{})
}

func (s *ClientE2ESuite) TestLock() {
	t := s.T()
	rdb := s.rdb
	testCases := []struct {
		name string

		key        string
		expiration time.Duration
		retry      func() retry.Strategy
		timeout    time.Duration

		wantLock *Lock
		wantErr  error

		before func()
		after  func()
	}{
		{
			// 一次加锁成功
			name: "locked",
			key:  "locked-key",
			retry: func() retry.Strategy {
				res, _ := retry.NewFixedIntervalRetryStrategy(time.Second, 3)
				return res
			},
			timeout:    time.Second,
			expiration: time.Minute,
			before:     func() {},
			after: func() {
				res, err := rdb.Del(context.Background(), "locked-key").Result()
				require.NoError(t, err)
				require.Equal(t, int64(1), res)
			},
		},
		{
			// 加锁不成功
			name: "failed",
			key:  "failed-key",
			retry: func() retry.Strategy {
				res, _ := retry.NewFixedIntervalRetryStrategy(time.Second, 3)
				return res
			},
			timeout:    time.Second,
			expiration: time.Minute,
			before: func() {
				res, err := rdb.Set(context.Background(), "failed-key", "123", time.Minute).Result()
				require.NoError(t, err)
				require.Equal(t, "OK", res)
			},
			after: func() {
				res, err := rdb.Get(context.Background(), "failed-key").Result()
				require.NoError(t, err)
				require.Equal(t, "123", res)
				delRes, err := rdb.Del(context.Background(), "failed-key").Result()
				require.NoError(t, err)
				require.Equal(t, int64(1), delRes)
			},
			wantErr: errs.ErrLocked,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before()
			lock := NewLock(rdb, tc.key, tc.expiration,
				WithLockTimeout(tc.timeout), WithLockRetryStrategy(tc.retry()))
			err := lock.Lock(context.Background())
			assert.True(t, errors.Is(err, tc.wantErr))
			if err != nil {
				return
			}
			assert.NotEmpty(t, lock.value)
			tc.after()
		})
	}
}

func (s *ClientE2ESuite) TestRefresh() {
	t := s.T()
	rdb := s.rdb
	testCases := []struct {
		name string

		timeout time.Duration
		lock    *Lock

		wantErr error

		before func()
		after  func()
	}{
		{
			name: "refresh success",
			lock: &Lock{
				key:        "refresh-key",
				value:      "123",
				expiration: time.Minute,
				client:     rdb,
			},
			before: func() {
				// 设置一个比较短的过期时间
				res, err := rdb.SetNX(context.Background(),
					"refresh-key", "123", time.Second*10).Result()
				require.NoError(t, err)
				assert.True(t, res)
			},
			after: func() {
				res, err := rdb.TTL(context.Background(), "refresh-key").Result()
				require.NoError(t, err)
				// 刷新完过期时间
				assert.True(t, res.Seconds() > 50)
				// 清理数据
				rdb.Del(context.Background(), "refresh-key")
			},
			timeout: time.Minute,
		},
		{
			// 锁被人持有
			name: "refresh failed",
			lock: &Lock{
				key:        "refresh-key",
				value:      "123",
				expiration: time.Minute,
				client:     rdb,
			},
			before: func() {
				// 设置一个比较短的过期时间
				res, err := rdb.SetNX(context.Background(),
					"refresh-key", "456", time.Second*10).Result()
				require.NoError(t, err)
				assert.True(t, res)
			},
			after: func() {
				res, err := rdb.Get(context.Background(), "refresh-key").Result()
				require.NoError(t, err)
				require.Equal(t, "456", res)
				rdb.Del(context.Background(), "refresh-key")
			},
			timeout: time.Minute,
			wantErr: errs.ErrLocked,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before()
			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			err := tc.lock.Refresh(ctx)
			cancel()
			assert.Equal(t, tc.wantErr, err)
			tc.after()
		})
	}
}

func (s *ClientE2ESuite) TestUnLock() {
	t := s.T()
	rdb := s.rdb
	testCases := []struct {
		name string

		lock *Lock

		before func()
		after  func()

		wantLock *Lock
		wantErr  error
	}{
		{
			name: "unlocked",
			lock: func() *Lock {
				lock := NewLock(rdb, "unlocked-key", time.Minute)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				err := lock.Lock(ctx)
				assert.NoError(t, err)
				return lock
			}(),
			before: func() {},
			after: func() {
				res, err := rdb.Exists(context.Background(), "unlocked-key").Result()
				require.NoError(t, err)
				require.Equal(t, 1, res)
			},
		},
		{
			name:    "lock not hold",
			lock:    NewLock(rdb, "not-hold-key", time.Minute),
			wantErr: errs.ErrLocked,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.lock.Unlock(context.Background())
			require.Equal(t, tc.wantErr, err)
		})
	}
}
