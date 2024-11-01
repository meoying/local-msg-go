package service

import (
	"context"
	"encoding/json"
	"github.com/IBM/sarama"
	"github.com/meoying/local-msg-go/internal/dao"
	"github.com/meoying/local-msg-go/internal/msg"
	"github.com/meoying/local-msg-go/internal/sharding"
	"gorm.io/gorm"
	"log/slog"
	"time"
)

// ShardingService 支持分库分表操作的
type ShardingService struct {
	// 分库之后的连接信息
	// 注意的是，多个逻辑库可以共享一个连接信息
	DBs map[string]*gorm.DB
	// 分库分表规则
	Sharding sharding.Sharding

	// WaitDuration 承担两方面的责任
	// 1. 如果超过这个时间，还处于初始状态，就认为业务发送消息失败，需要补偿
	// 2. 当发送一次失败之后，要过这么久才会重试
	WaitDuration time.Duration

	Producer sarama.SyncProducer
	MaxTimes int

	Logger *slog.Logger
}

func NewService(dbs map[string]*gorm.DB, producer sarama.SyncProducer, sharding sharding.Sharding, waitDuration time.Duration, maxTimes int, logger *slog.Logger) *ShardingService {
	return &ShardingService{DBs: dbs, Producer: producer, Sharding: sharding, WaitDuration: waitDuration, MaxTimes: maxTimes, Logger: logger}
}

func (svc *ShardingService) StartAsyncTask(ctx context.Context) {
	go func() {
		for _, dst := range svc.Sharding.EffectiveTablesFunc() {
			task := AsyncTask{
				waitDuration: svc.WaitDuration,
				svc:          svc,
				db:           svc.DBs[dst.DB],
				dst:          dst,
			}
			go func() {
				task.Start(ctx)
			}()
		}
	}()
}

// ExecTx 闭包接口。biz 是你要执行的业务代码，msg 则是消息
// 因为这是服务于分库分表的，所以需要构造好本地消息，才能知道应该插入哪个数据库
func (svc *ShardingService) ExecTx(ctx context.Context,
	msg msg.Msg,
	biz func(tx *gorm.DB) error) error {

	dmsg := svc.newDmsg(msg)
	dst := svc.Sharding.ShardingFunc(msg)
	db := svc.DBs[dst.DB]
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := biz(tx)
		if err != nil {
			return err
		}
		return tx.Create(dmsg).Error
	})
	if err == nil {
		svc.sendMsg(ctx, db, dmsg)
	}
	return err
}

func (svc *ShardingService) sendMsg(ctx context.Context, db *gorm.DB, dmsg *dao.LocalMsg) {
	var msg msg.Msg
	err := json.Unmarshal(dmsg.Data, &msg)
	if err != nil {
		svc.Logger.Error("提取消息内容失败", err)
		return
	}
	// 发送消息
	_, _, err = svc.Producer.SendMessage(newSaramaProducerMsg(msg))
	times := dmsg.SendTimes + 1
	var fields = map[string]interface{}{
		"utime":  time.Now().UnixMilli(),
		"retry":  times,
		"status": dao.MsgStatusInit,
	}
	if err != nil {
		svc.Logger.Error("发送消息失败",
			slog.String("topic", msg.Topic),
			slog.String("key", msg.Key),
			slog.Int("retry", dmsg.SendTimes),
		)
		if times == svc.MaxTimes {
			fields["status"] = dao.MsgStatusFail
		}
	} else {
		fields["status"] = dao.MsgStatusSuccess
	}

	err1 := db.WithContext(ctx).Where("id=?", dmsg.Id).
		Updates(fields).Error
	if err1 != nil {
		svc.Logger.Error("发送消息但是更新消息失败",
			// 是否发送成功
			slog.Bool("success", err == nil),
			slog.String("topic", msg.Topic),
			slog.String("key", msg.Key),
			slog.Int("retry", dmsg.SendTimes),
		)
	}
}

func (svc *ShardingService) newDmsg(msg msg.Msg) *dao.LocalMsg {
	val, _ := json.Marshal(msg)
	now := time.Now().UnixMilli()
	return &dao.LocalMsg{
		Data:   val,
		Status: dao.MsgStatusInit,
		Utime:  now,
		Ctime:  now,
	}
}
