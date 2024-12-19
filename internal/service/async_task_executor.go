package service

import (
	"context"
	"fmt"
	"github.com/ecodeclub/ekit/slice"
	"github.com/meoying/local-msg-go/internal/dao"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
	"log/slog"
	"sync/atomic"
	"time"
)

// Executor 补偿任务执行器
type Executor interface {
	// Exec 第一个返回值处理了多少数据 第二个返回值最终失败了几个
	Exec(ctx context.Context, db *gorm.DB, table string) (int, int, error)
}

func findSuspendMsg(ctx context.Context, db *gorm.DB, waitDuration time.Duration, table string,
	offset, limit int) ([]dao.LocalMsg, error) {
	now := time.Now().UnixMilli()
	utime := now - waitDuration.Milliseconds()
	var res []dao.LocalMsg
	// 这不用检测重试次数，因为重试次数达到了的话，状态会修改
	err := db.WithContext(ctx).
		// 考虑到分库分表的问题，这里需要指定表名
		Table(table).
		Where("status=? AND utime < ?", dao.MsgStatusInit, utime).
		Limit(limit).Offset(offset).Find(&res).Error
	return res, err
}

// CurMsgExecutor 并发发送消息
type CurMsgExecutor struct {
	svc    *ShardingService
	logger *slog.Logger
}

func NewCurMsgExecutor(svc *ShardingService) *CurMsgExecutor {
	return &CurMsgExecutor{
		svc:    svc,
		logger: svc.Logger,
	}
}

func (c *CurMsgExecutor) Exec(ctx context.Context, db *gorm.DB, table string) (int, int, error) {
	data, err := findSuspendMsg(ctx, db, c.svc.WaitDuration, table, 0, c.svc.BatchSize)
	if err != nil {
		c.logger.Error("查询数据失败", slog.String("err", err.Error()))
		return 0, 0, fmt.Errorf("查询数据失败 %w", err)
	}
	c.logger.Debug("找到数据", slog.Int("cnt", len(data)))
	var eg errgroup.Group
	var count int64
	for _, m := range data {
		shadow := m
		eg.Go(func() error {
			failCount, err1 := c.svc.sendMsg(ctx, db, &shadow, table)
			if err1 != nil {
				atomic.AddInt64(&count, int64(failCount))
				err1 = fmt.Errorf("发送消息失败 %w", err1)
			}
			return err1
		})
	}
	return len(data), int(count), eg.Wait()
}

// BatchMsgExecutor 批量发送消息
type BatchMsgExecutor struct {
	svc    *ShardingService
	logger *slog.Logger
}

func NewBatchMsgExecutor(svc *ShardingService) *BatchMsgExecutor {
	return &BatchMsgExecutor{
		svc:    svc,
		logger: svc.Logger,
	}
}

func (b *BatchMsgExecutor) Exec(ctx context.Context, db *gorm.DB, table string) (int, int, error) {
	data, err := findSuspendMsg(ctx, db, b.svc.WaitDuration, table, 0, b.svc.BatchSize)
	if err != nil {
		b.logger.Error("查询数据失败", slog.String("err", err.Error()))
		return 0, 0, fmt.Errorf("查询数据失败 %w", err)
	}
	b.logger.Debug("找到数据", slog.Int("cnt", len(data)))
	failCount, err := b.svc.sendMsgs(ctx, db, getMsgs(data), table)
	if err != nil {
		return 0, int(failCount), fmt.Errorf("发送消息失败 %w", err)
	}
	return len(data), int(failCount), nil
}

func getMsgs(dmsgs []dao.LocalMsg) []*dao.LocalMsg {
	return slice.Map(dmsgs, func(idx int, src dao.LocalMsg) *dao.LocalMsg {
		return &src
	})
}
