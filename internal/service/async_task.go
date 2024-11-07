package service

import (
	"context"
	"errors"
	"github.com/IBM/sarama"
	"github.com/meoying/local-msg-go/internal/dao"
	"github.com/meoying/local-msg-go/internal/msg"
	"github.com/meoying/local-msg-go/internal/sharding"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
	"log/slog"
	"time"
)

// AsyncTask 异步任务
// 一个表一个异步任务
type AsyncTask struct {
	waitDuration time.Duration
	dst          sharding.Dst
	svc          *ShardingService
	db           *gorm.DB

	logger *slog.Logger

	batchSize int
}

// Start 开启补偿任务。当 ctx 过期或者被取消的时候，就会退出
func (task *AsyncTask) Start(ctx context.Context) {
	// 开启
	for {
		cnt := task.loop(ctx)
		ctxErr := ctx.Err()
		switch {
		case errors.Is(ctxErr, context.Canceled), errors.Is(ctxErr, context.DeadlineExceeded):
			// 被取消，那么就要跳出循环
			task.logger.Info("任务被取消，退出任务循环")
			return
		case ctxErr != nil:
			task.logger.Error("补偿任务循环失败", ctxErr)
		default:
			// 一条都没有取到。那就说明没数据了，稍微等一下
			if ctxErr == nil && cnt == 0 {
				time.Sleep(time.Second)
			}
		}
	}
}

func (task *AsyncTask) loop(ctx context.Context) int {
	// 假设是 3s 一个循环，这个参数也可以控制
	loopCtx, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()
	// 因为你每次都会更新时间和状态，所以你可以永远从 0 开始
	data, err := task.findSuspendMsg(loopCtx, 0, task.batchSize)
	if err != nil {
		task.logger.Error("查询数据失败", slog.Any("err", err))
		return 0
	}
	task.logger.Debug("找到数据", slog.Int("cnt", len(data)))
	var eg errgroup.Group
	for _, msg := range data {
		shadow := msg
		eg.Go(func() error {
			err1 := task.svc.sendMsg(loopCtx, task.db, &shadow, task.dst.Table)
			if err1 != nil {
				slog.Error("发送消息失败", slog.Any("err", err1))
			}
			return nil
		})
	}
	_ = eg.Wait()
	return len(data)
}

func (task *AsyncTask) findSuspendMsg(ctx context.Context,
	offset, limit int) ([]dao.LocalMsg, error) {
	now := time.Now().UnixMilli()
	utime := now - task.waitDuration.Milliseconds()
	var res []dao.LocalMsg
	// 这不用检测重试次数，因为重试次数达到了的话，状态会修改
	err := task.db.WithContext(ctx).
		// 考虑到分库分表的问题，这里需要指定表名
		Table(task.dst.Table).
		Where("status=? AND utime < ?", dao.MsgStatusInit, utime).
		Limit(limit).Offset(offset).Find(&res).Error
	return res, err
}

func newSaramaProducerMsg(m msg.Msg) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic:     m.Topic,
		Partition: m.Partition,
		Key:       sarama.StringEncoder(m.Key),
		Value:     sarama.ByteEncoder(m.Content),
	}
}
