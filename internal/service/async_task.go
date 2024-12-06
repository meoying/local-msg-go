package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/meoying/local-msg-go/internal/dao"
	dlock "github.com/meoying/local-msg-go/internal/lock"
	"github.com/meoying/local-msg-go/internal/lock/errs"
	"github.com/meoying/local-msg-go/internal/msg"
	"github.com/meoying/local-msg-go/internal/sharding"
	"gorm.io/gorm"
	"log/slog"
	"time"
)

// AsyncTask 异步任务
// 一个表一个异步任务
type AsyncTask struct {
	waitDuration time.Duration
	dst          sharding.Dst
	msgSender			MsgSender
	db           *gorm.DB

	logger *slog.Logger

	batchSize int

	lockClient dlock.Client
}
// Start 开启补偿任务。当 ctx 过期或者被取消的时候，就会退出
func (task *AsyncTask) Start(ctx context.Context) {
	key := fmt.Sprintf("%s.%s", task.dst.DB, task.dst.Table)
	task.logger = task.logger.With(slog.String("key", key))
	interval := time.Minute
	for {
		// 每个循环过程就是一次尝试拿到分布式锁之后，不断调度的过程
		lock, err := task.lockClient.NewLock(ctx, key, interval)
		if err != nil {
			task.logger.Error("初始化分布式锁失败，重试",
				slog.Any("err", err))
			// 暂停一会
			time.Sleep(interval)
			continue
		}

		lockCtx, cancel := context.WithTimeout(ctx, time.Second*3)
		// 没有拿到锁，不管是系统错误，还是锁被人持有，都没有关系
		// 暂停一段时间之后继续
		err = lock.Lock(lockCtx)
		cancel()

		if err != nil {
			if errors.Is(err, errs.ErrLocked) {
				task.logger.Info("没有抢到分布式锁，此刻正有人持有锁")
			} else {
				task.logger.Error("没有抢到分布式锁，系统出现问题", slog.Any("err", err))
			}
			// 10 秒钟是一个比较合适的
			time.Sleep(interval)
			continue
		}
		// 开启任务循环
		task.refreshAndLoop(ctx, lock)
		// 只要这个方法返回，就说明你需要释放掉分布式锁，
		// 比如说因为负载、异常等问题，导致你已经无法继续执行下去了
		if unErr := lock.Unlock(ctx); unErr != nil {
			task.logger.Error("释放分布式锁失败", slog.Any("err", unErr.Error()))
		}
		// 从这里退出的时候，要检测一下是不是需要结束了
		ctxErr := ctx.Err()
		switch {
		case errors.Is(ctxErr, context.Canceled), errors.Is(ctxErr, context.DeadlineExceeded):
			// 被取消，那么就要跳出循环
			task.logger.Info("任务被取消，退出任务循环")
			return
		default:
			task.logger.Error("执行补偿任务失败，将执行重试")
			time.Sleep(interval)
		}
	}
	// 开启
}

func (task *AsyncTask) refreshAndLoop(ctx context.Context, lock dlock.Lock) {
	// 这里有两个动作：
	// 1. 执行异步补偿任务，
	// 2. 判定要不要让出分布式锁，这里采用一种比较简单的策略，
	// 3. 即当自身执行出错比较高的时候，就让出分布式锁

	// 连续出现 error 的次数，用于容错、负载均衡
	errCnt := 0
	for {
		// 刷新的过期时间应该很短
		refreshCtx, cancel := context.WithTimeout(ctx, time.Second*3)
		// 手动控制每一批次循环开始就续约分布式锁
		// 而后控制每一批次的超时时间，
		// 这样就可以确保不会出现分布式锁过期，但是任务还在运行的情况
		err := lock.Refresh(refreshCtx)
		cancel()
		if err != nil {
			slog.Error("分布式锁续约失败，直接返回")
			return
		}

		cnt, err := task.loop(ctx)
		ctxErr := ctx.Err()
		switch {
		case errors.Is(ctxErr, context.Canceled), errors.Is(ctxErr, context.DeadlineExceeded):
			// 这个是最上层用户取消，一般就是关闭服务的时候会触发
			return
		case err != nil:
			// 说明执行出错了，这个时候我们认为可能是偶发性失败，
			// 也可能是系统高负载引起不可用
			// 也可能是彻底不可用，我们通过连续 N 次循环都出错来判定是偶发还是非偶发
			errCnt++
			// 连续 5 次，基本上可以断定不是偶发性错误了
			// 连续次数越多，越容易避开偶发性错误
			const threshold = 5
			if errCnt >= threshold {
				task.logger.Error("执行任务连续出错，退出循环", slog.Int("threshold", threshold))
				return
			}
		default:
			// 重置
			errCnt = 0
			// 一条都没有取到。那就说明没数据了，稍微等一下
			if cnt == 0 {
				time.Sleep(time.Second)
			}
		}
	}
}

func (task *AsyncTask) loop(ctx context.Context) (int, error) {
	// 假设是 3s 一个循环，这个参数也可以控制
	loopCtx, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()
	// 因为你每次都会更新时间和状态，所以你可以永远从 0 开始
	data, err := task.findSuspendMsg(loopCtx, 0, task.batchSize)
	if err != nil {
		task.logger.Error("查询数据失败", slog.String("err", err.Error()))
		return 0, fmt.Errorf("查询数据失败 %w", err)
	}
	task.logger.Debug("找到数据", slog.Int("cnt", len(data)))
	err = task.msgSender.SendMsg(ctx,task.db,data,task.dst.Table)
	return len(data), err
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
