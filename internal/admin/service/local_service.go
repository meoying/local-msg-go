package service

import (
	"context"
	"encoding/json"
	"github.com/IBM/sarama"
	"github.com/ecodeclub/ekit/slice"
	"github.com/meoying/local-msg-go/internal/dao"
	"github.com/meoying/local-msg-go/internal/msg"
	"github.com/meoying/local-msg-go/internal/service"
	"time"
)

// LocalService 是和服务一起部署的时候使用的实现
type LocalService struct {
	svcs     map[string]*service.ShardingService
	daos     map[string]map[string]*dao.MsgDAO
	producer sarama.SyncProducer
}

func NewLocalService(producer sarama.SyncProducer) *LocalService {
	return &LocalService{
		daos:     make(map[string]map[string]*dao.MsgDAO),
		producer: producer,
		svcs:     make(map[string]*service.ShardingService, 4),
	}
}

func (svc *LocalService) Register(biz string, s *service.Service) error {
	return svc.RegisterShardingSvc(biz, s.ShardingService)
}

func (svc *LocalService) RegisterShardingSvc(biz string, s *service.ShardingService) error {
	m, ok := svc.daos[biz]
	if !ok {
		m = make(map[string]*dao.MsgDAO)
	}
	for k, db := range s.DBs {
		m[k] = dao.NewMsgDAO(db)
	}
	svc.daos[biz] = m
	svc.svcs[biz] = s
	return nil
}

func (svc *LocalService) Retry(ctx context.Context,
	biz, db, table string,
	id int64) error {
	msgDAO := svc.getDAO(biz, db)
	localMsg, err := msgDAO.Get(ctx, table, id)
	if err != nil {
		return err
	}
	data := string(localMsg.Data)
	var m msg.Msg
	err = json.Unmarshal([]byte(data), &m)
	if err != nil {
		return err
	}
	return svc.svcs[biz].SendMsg(ctx, db, table, m)
}

// ListMsgs 返回未发送的消息
func (svc *LocalService) ListMsgs(
	ctx context.Context,
	biz, db string, query Query) ([]LocalMsg, error) {
	msgDAO := svc.getDAO(biz, db)
	res, err := msgDAO.List(ctx, query)
	if err != nil {
		return nil, err
	}
	return slice.Map(res, func(idx int, src dao.LocalMsg) LocalMsg {
		var m msg.Msg
		_ = json.Unmarshal(src.Data, &m)
		return LocalMsg{
			Id:        src.Id,
			Msg:       m,
			Key:       src.Key,
			Status:    src.Status,
			SendTimes: src.SendTimes,
			Ctime:     time.UnixMilli(src.Ctime),
			Utime:     time.UnixMilli(src.Utime),
		}
	}), nil
}

func (svc *LocalService) getDAO(biz, db string) *dao.MsgDAO {
	return svc.daos[biz][db]
}

type LocalMsg struct {
	Id        int64
	Msg       msg.Msg
	Key       string
	Status    int8
	SendTimes int
	Ctime     time.Time
	Utime     time.Time
}
