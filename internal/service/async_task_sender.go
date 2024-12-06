package service

import (
	"context"
	"fmt"
	"github.com/ecodeclub/ekit/slice"
	"github.com/meoying/local-msg-go/internal/dao"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
)
// MsgSender 补偿任务用于发送消息的抽象 目前提供两个实现，一个是并发发送，一个是批量发送
type MsgSender interface {
	SendMsg(ctx context.Context, db *gorm.DB, dmsgs []dao.LocalMsg, table string) error
}

// CurMsgSender 并发发送
type CurMsgSender struct {
	svc          *ShardingService
}

func NewCurMsgSender(svc *ShardingService)MsgSender {
	return &CurMsgSender{
		svc: svc,
	}
}



func (c *CurMsgSender) SendMsg(ctx context.Context, db *gorm.DB, dmsgs []dao.LocalMsg, table string) error {
	var eg errgroup.Group
	for _, m := range dmsgs {
		shadow := m
		eg.Go(func() error {
			err1 := c.svc.sendMsg(ctx, db, &shadow, table)
			if err1 != nil {
				err1 = fmt.Errorf("发送消息失败 %w", err1)
			}
			return err1
		})
	}
	return eg.Wait()
}


func getMsgs(dmsgs []dao.LocalMsg)[]*dao.LocalMsg{
	return slice.Map(dmsgs, func(idx int, src dao.LocalMsg) *dao.LocalMsg {
		return &src
	})
}


// BatchMsgSender 批量发送
type BatchMsgSender struct {
	svc          *ShardingService
}

func NewBatchMsgSender(svc *ShardingService)MsgSender{
	return &BatchMsgSender{
		svc: svc,
	}
}

func (b *BatchMsgSender) SendMsg(ctx context.Context, db *gorm.DB, dmsgs []dao.LocalMsg, table string) error {
	err := b.svc.sendMsgs(ctx, db, getMsgs(dmsgs), table)
	if err != nil {
		return  fmt.Errorf("发送消息失败 %w", err)
	}
	return nil
}
