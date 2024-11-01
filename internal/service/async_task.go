package service

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/meoying/local-msg-go/internal/dao"
	"github.com/meoying/local-msg-go/internal/msg"
	"github.com/meoying/local-msg-go/internal/sharding"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
	"time"
)

// AsyncTask 异步任务
// 一个表一个异步任务
type AsyncTask struct {
	waitDuration time.Duration
	dst          sharding.Dst
	svc          *ShardingService
	db           *gorm.DB
}

// Start 开启补偿任务。当 ctx 过期或者被取消的时候，就会退出
func (task *AsyncTask) Start(ctx context.Context) {
	// 开启
	for {
		cnt := task.loop(ctx)
		if cnt == 0 {
			time.Sleep(time.Second)
		}
	}
}

func (task *AsyncTask) loop(ctx context.Context) int {
	// 假设是 3s 一个循环，这个参数也可以控制
	loopCtx, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()
	// 10 条一批，因为你每次都会更新时间和状态，所以你可以永远从 0 开始
	data, err := task.findSuspendMsg(loopCtx, 0, 10)
	if err != nil {
		task.svc.Logger.Error("查找待发送消息失败", err)
		return 0
	}
	var eg errgroup.Group
	for _, msg := range data {
		eg.Go(func() error {
			task.svc.sendMsg(loopCtx, task.db, &msg)
			return nil
		})
	}
	return len(data)
}

func (task *AsyncTask) findSuspendMsg(ctx context.Context, offset, limit int) ([]dao.LocalMsg, error) {
	now := time.Now().Unix()
	utime := now - task.waitDuration.Milliseconds()
	var res []dao.LocalMsg
	// 这不用检测重试次数，因为重试次数达到了的话，状态会修改
	err := task.db.WithContext(ctx).
		// 考虑到分库分表的问题，这里需要指定表名
		Table(task.dst.TableName()).
		Where("status=? AND utime < ?", utime).
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
