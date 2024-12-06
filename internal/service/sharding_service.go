package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/ecodeclub/ekit/slice"
	"github.com/meoying/local-msg-go/internal/dao"
	dlock "github.com/meoying/local-msg-go/internal/lock"
	"github.com/meoying/local-msg-go/internal/msg"
	"github.com/meoying/local-msg-go/internal/sharding"
	"github.com/pkg/errors"
	"gorm.io/gorm"
	"log/slog"
	"strings"
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
	// 用于补充任务发送消息
	sender    MsgSender
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
	sharding sharding.Sharding, opts ...ShardingServiceOpt) *ShardingService {
	svc := &ShardingService{
		DBs:          dbs,
		Producer:     producer,
		Sharding:     sharding,
		WaitDuration: time.Second * 30,
		MaxTimes:     3,
		BatchSize:    10,
		Logger:       slog.Default(),
		LockClient:   lockClient,
	}
	// 默认为并发发送
	svc.sender = NewCurMsgSender(svc)
	for _, opt := range opts {
		opt(svc)
	}
	return svc
}

func WithBatchSender() ShardingServiceOpt {
	return func(service *ShardingService) {
		service.sender = NewBatchMsgSender(service)
	}
}

type ShardingServiceOpt func(service *ShardingService)

func (svc *ShardingService) StartAsyncTask(ctx context.Context) {

	for _, dst := range svc.Sharding.EffectiveTablesFunc() {
		task := AsyncTask{
			waitDuration: svc.WaitDuration,
			msgSender:    svc.sender,
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

// BatchSendMsg 批量发送消息
func (svc *ShardingService) BatchSendMsg(ctx context.Context, db, table string, msgs ...msg.Msg) error {
	dmsgs := slice.Map(msgs, func(idx int, src msg.Msg) *dao.LocalMsg {
		return svc.newDmsg(src)
	})
	return svc.sendMsgs(ctx, svc.DBs[db], dmsgs, table)
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

func (svc *ShardingService) sendMsgs(ctx context.Context,
	db *gorm.DB, dmsgs []*dao.LocalMsg, table string) error {
	msgs := make([]msg.Msg, 0, len(dmsgs))
	// 这个方法的前提是发送到同一个topic
	var topic string
	for _, dmsg := range dmsgs {
		var msg msg.Msg
		err := json.Unmarshal(dmsg.Data, &msg)
		if err != nil {
			return fmt.Errorf("提取消息内容失败 %w", err)
		}
		topic = msg.Topic
		msgs = append(msgs, msg)
	}
	// 发送消息
	err := svc.Producer.SendMessages(slice.Map(msgs, func(idx int, src msg.Msg) *sarama.ProducerMessage {
		return newSaramaProducerMsg(src)
	}))

	failMsgs := make([]*dao.LocalMsg, 0)
	initMsgs := make([]*dao.LocalMsg, 0)
	successMsgs := make([]*dao.LocalMsg, 0)
	failFields := map[string]any{
		"utime":      time.Now().UnixMilli(),
		"send_times": gorm.Expr("send_times + 1"),
		"status":     dao.MsgStatusFail,
	}
	initFields := map[string]any{
		"utime":      time.Now().UnixMilli(),
		"send_times": gorm.Expr("send_times + 1"),
		"status":     dao.MsgStatusInit,
	}
	successFields := map[string]any{
		"utime":      time.Now().UnixMilli(),
		"send_times": gorm.Expr("send_times + 1"),
		"status":     dao.MsgStatusSuccess,
	}
	if err != nil {
		var errs sarama.ProducerErrors
		if errors.As(err, &errs) {
			var perr error
			failMsgs, initMsgs, successMsgs, perr = svc.getPartitionMsgs(dmsgs, errs)
			if perr != nil {
				return perr
			}
		} else {
			failMsgs, initMsgs = svc.getFailMsgs(dmsgs)
		}
	} else {
		successMsgs = dmsgs
	}
	if len(successMsgs) > 0 {

		err = svc.updateMsgs(ctx, db, successMsgs, successFields, topic, table)
		if err != nil {
			return err
		}
	}
	if len(failMsgs) > 0 {
		svc.Logger.Error("发送消息失败",
			slog.String("topic", topic),
			slog.String("keys", svc.getKeyStr(successMsgs)),
		)
		err = svc.updateMsgs(ctx, db, failMsgs, failFields, topic, table)
		if err != nil {
			return err
		}
	}
	if len(initMsgs) > 0 {
		svc.Logger.Error("发送消息失败",
			slog.String("topic", topic),
			slog.String("keys", svc.getKeyStr(successMsgs)),
		)
		err = svc.updateMsgs(ctx, db, initMsgs, initFields, topic, table)
		if err != nil {
			return err
		}
	}
	return nil
}

// 部分失败获取消息
func (svc *ShardingService) getPartitionMsgs(dmsgs []*dao.LocalMsg, errMsgs sarama.ProducerErrors) ([]*dao.LocalMsg, []*dao.LocalMsg, []*dao.LocalMsg, error) {
	failMsgs := make([]*dao.LocalMsg, 0, len(errMsgs))
	initMsgs := make([]*dao.LocalMsg, 0, len(errMsgs))
	successMsgs := make([]*dao.LocalMsg, 0, len(dmsgs)-len(errMsgs))
	failMsgMap := make(map[string]struct{})
	for _, errMsg := range errMsgs {
		keyByte, err := errMsg.Msg.Key.Encode()
		if err != nil {
			return nil, nil, nil, fmt.Errorf("提取消息内容失败 %w", err)
		}
		failMsgMap[string(keyByte)] = struct{}{}
	}
	for _, dmsg := range dmsgs {
		if _, ok := failMsgMap[dmsg.Key]; ok {
			if dmsg.SendTimes+1 >= svc.MaxTimes {
				failMsgs = append(failMsgs, dmsg)
			} else {
				initMsgs = append(initMsgs, dmsg)
			}
		} else {
			successMsgs = append(successMsgs, dmsg)
		}
	}
	return failMsgs, initMsgs, successMsgs, nil

}

// 完全失败获取消息
func (svc *ShardingService) getFailMsgs(dmsgs []*dao.LocalMsg) ([]*dao.LocalMsg, []*dao.LocalMsg) {
	failMsgs := make([]*dao.LocalMsg, 0, len(dmsgs))
	initMsgs := make([]*dao.LocalMsg, 0, len(dmsgs))
	for _, dmsg := range dmsgs {
		if dmsg.SendTimes+1 >= svc.MaxTimes {
			failMsgs = append(failMsgs, dmsg)
		} else {
			initMsgs = append(initMsgs, dmsg)
		}
	}
	return failMsgs, initMsgs
}

func (svc *ShardingService) updateMsgs(ctx context.Context, db *gorm.DB, dmsgs []*dao.LocalMsg, fieldMap map[string]any, topic, table string) error {
	err1 := db.WithContext(ctx).Model(&dao.LocalMsg{}).Table(table).
		Where("`key` in ?", svc.getKeys(dmsgs)).
		Updates(fieldMap).Error
	if err1 != nil {
		return fmt.Errorf("发送消息但是更新消息失败 %w, topic %s, keys %s",
			err1, topic, svc.getKeys(dmsgs))
	}
	return nil
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

func (svc *ShardingService) getKeyStr(dmsgs []*dao.LocalMsg) string {
	keyList := slice.Map(dmsgs, func(idx int, src *dao.LocalMsg) string {
		return src.Key
	})
	return strings.Join(keyList, ",")
}

func (svc *ShardingService) getIds(dmsgs []*dao.LocalMsg) []int64 {
	ids := slice.Map(dmsgs, func(idx int, src *dao.LocalMsg) int64 {
		return src.Id
	})
	return ids
}

func (svc *ShardingService) getKeys(dmsgs []*dao.LocalMsg) []string {
	return slice.Map(dmsgs, func(idx int, src *dao.LocalMsg) string {
		return src.Key
	})
}
