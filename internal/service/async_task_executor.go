package service

import (
	"context"
	"fmt"
	"github.com/ecodeclub/ekit/slice"
	"github.com/meoying/local-msg-go/internal/dao"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
	"log/slog"
	"time"
)

// Executor 补偿任务执行器
type Executor interface {
	Exec(ctx context.Context, db *gorm.DB, table string) (int, error)
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
	svc          *ShardingService
	logger       *slog.Logger
}

func NewCurMsgExecutor(svc *ShardingService) *CurMsgExecutor {
	return &CurMsgExecutor{
		svc:          svc,
		logger:       svc.Logger,
	}
}

func (c *CurMsgExecutor) Exec(ctx context.Context, db *gorm.DB, table string) (int, error) {
	data, err := findSuspendMsg(ctx, db, c.svc.WaitDuration, table, 0, c.svc.BatchSize)
	if err != nil {
		c.logger.Error("查询数据失败", slog.String("err", err.Error()))
		return 0, fmt.Errorf("查询数据失败 %w", err)
	}
	c.logger.Debug("找到数据", slog.Int("cnt", len(data)))
	var eg errgroup.Group
	for _, m := range data {
		shadow := m
		eg.Go(func() error {
			err1 := c.svc.sendMsg(ctx, db, &shadow, table)
			if err1 != nil {
				err1 = fmt.Errorf("发送消息失败 %w", err1)
			}
			return err1
		})
	}
	return len(data), eg.Wait()
}

// BatchMsgExecutor 批量发送消息
type BatchMsgExecutor struct {
	svc          *ShardingService
	logger       *slog.Logger
}

func NewBatchMsgExecutor(svc *ShardingService) *BatchMsgExecutor {
	return &BatchMsgExecutor{
		svc:          svc,
		logger:       svc.Logger,
	}
}

func (b *BatchMsgExecutor) Exec(ctx context.Context, db *gorm.DB, table string) (int, error) {
	data, err := findSuspendMsg(ctx, db, b.svc.WaitDuration, table, 0, b.svc.BatchSize)
	if err != nil {
		b.logger.Error("查询数据失败", slog.String("err", err.Error()))
		return 0, fmt.Errorf("查询数据失败 %w", err)
	}
	b.logger.Debug("找到数据", slog.Int("cnt", len(data)))
	err = b.svc.sendMsgs(ctx, db, getMsgs(data), table)
	if err != nil {
		return 0, fmt.Errorf("发送消息失败 %w", err)
	}
	return len(data), nil
}

func getMsgs(dmsgs []dao.LocalMsg)[]*dao.LocalMsg{
	return slice.Map(dmsgs, func(idx int, src dao.LocalMsg) *dao.LocalMsg {
		return &src
	})
}