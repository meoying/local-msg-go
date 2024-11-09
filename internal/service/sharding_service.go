package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/meoying/local-msg-go/internal/dao"
	dlock "github.com/meoying/local-msg-go/internal/lock"
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

	Producer  sarama.SyncProducer
	MaxTimes  int
	BatchSize int

	LockClient dlock.Client

	Logger *slog.Logger
}

func NewShardingService(
	dbs map[string]*gorm.DB,
	producer sarama.SyncProducer,
	lockClient dlock.Client,
	sharding sharding.Sharding) *ShardingService {
	return &ShardingService{
		DBs:          dbs,
		Producer:     producer,
		Sharding:     sharding,
		WaitDuration: time.Second * 30,
		MaxTimes:     3,
		BatchSize:    10,
		Logger:       slog.Default(),
		LockClient:   lockClient,
	}
}

func (svc *ShardingService) StartAsyncTask(ctx context.Context) {
	for _, dst := range svc.Sharding.EffectiveTablesFunc() {
		task := AsyncTask{
			waitDuration: svc.WaitDuration,
			svc:          svc,
			db:           svc.DBs[dst.DB],
			dst:          dst,
			batchSize:    svc.BatchSize,
			logger:       svc.Logger,
			lockClient:   svc.LockClient,
		}
		go func() {
			task.Start(ctx)
		}()
	}
}

// SendMsg 发送消息
func (svc *ShardingService) SendMsg(ctx context.Context, db, table string, msg msg.Msg) error {
	dmsg := svc.newDmsg(msg)
	return svc.sendMsg(ctx, svc.DBs[db], dmsg, table)
}

// SaveMsg 手动保存接口, tx 必须是你的本地事务
func (svc *ShardingService) SaveMsg(tx *gorm.DB, shardingInfo any, msg msg.Msg) error {
	dmsg := svc.newDmsg(msg)
	return tx.Create(&dmsg).Error
}

func (svc *ShardingService) execTx(ctx context.Context,
	db *gorm.DB,
	biz func(tx *gorm.DB) (msg.Msg, error),
	table string,
) error {
	var dmsg *dao.LocalMsg
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		m, err := biz(tx)
		dmsg = svc.newDmsg(m)
		if err != nil {
			return err
		}
		return tx.Table(table).Create(dmsg).Error
	})
	if err == nil {
		err1 := svc.sendMsg(ctx, db, dmsg, table)
		if err1 != nil {
			slog.Error("发送消息出现问题", slog.Any("error", err))
		}
	}
	return err
}

// ExecTx 闭包接口，优先考虑使用闭包接口。biz 是你要执行的业务代码，msg 则是消息
// 因为这是服务于分库分表的，所以需要构造好本地消息，才能知道应该插入哪个数据库
// 第二个参数你需要传入 msg，因为我们需要必要的信息来执行分库分表，找到目标库
// 第三个参数 biz 里面再次返回的 msg.Msg 会作为最终的消息内容，写入到数据库的，以及发送出去
func (svc *ShardingService) ExecTx(ctx context.Context,
	shardingInfo any,
	biz func(tx *gorm.DB) (msg.Msg, error)) error {
	dst := svc.Sharding.ShardingFunc(shardingInfo)
	db := svc.DBs[dst.DB]
	return svc.execTx(ctx, db, biz, dst.Table)
}

func (svc *ShardingService) sendMsg(ctx context.Context,
	db *gorm.DB, dmsg *dao.LocalMsg, table string) error {
	var msg msg.Msg
	err := json.Unmarshal(dmsg.Data, &msg)
	if err != nil {
		return fmt.Errorf("提取消息内容失败 %w", err)
	}
	// 发送消息
	_, _, err = svc.Producer.SendMessage(newSaramaProducerMsg(msg))
	times := dmsg.SendTimes + 1
	var fields = map[string]interface{}{
		"utime":      time.Now().UnixMilli(),
		"send_times": times,
		"status":     dao.MsgStatusInit,
	}
	if err != nil {
		svc.Logger.Error("发送消息失败",
			slog.String("topic", msg.Topic),
			slog.String("key", msg.Key),
			slog.Int("send_times", times),
		)
		if times >= svc.MaxTimes {
			fields["status"] = dao.MsgStatusFail
		}
	} else {
		fields["status"] = dao.MsgStatusSuccess
	}

	err1 := db.WithContext(ctx).Model(dmsg).Table(table).
		Where("id=?", dmsg.Id).
		Updates(fields).Error
	if err1 != nil {
		return fmt.Errorf("发送消息但是更新消息失败 %w, 发送结果 %w, topic %s, key %s",
			err1, err, msg.Topic, msg.Key)
	}
	return err
}

func (svc *ShardingService) newDmsg(msg msg.Msg) *dao.LocalMsg {
	val, _ := json.Marshal(msg)
	now := time.Now().UnixMilli()
	return &dao.LocalMsg{
		Data:      val,
		Status:    dao.MsgStatusInit,
		Key:       msg.Key,
		SendTimes: 0,
		Utime:     now,
		Ctime:     now,
	}
}
