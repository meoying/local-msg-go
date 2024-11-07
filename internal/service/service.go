package service

import (
	"context"
	"github.com/meoying/local-msg-go/internal/msg"
	"gorm.io/gorm"
)

// Service 不涉及分库分表的
type Service struct {
	*ShardingService
}

func (svc *Service) ExecTx(ctx context.Context,
	biz func(tx *gorm.DB) (msg.Msg, error)) error {
	db := svc.DBs[""]
	return svc.execTx(ctx, db, biz, "")
}
