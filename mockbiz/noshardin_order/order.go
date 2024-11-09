package noshardin_order

import (
	"context"
	"github.com/meoying/local-msg-go/internal/msg"
	"github.com/meoying/local-msg-go/internal/service"
	"gorm.io/gorm"
	"time"
)

// OrderService 模拟订单服务
type OrderService struct {
	db  *gorm.DB
	msg *service.Service
}

func NewOrderService(db *gorm.DB, msgSvc *service.Service) *OrderService {
	return &OrderService{db: db, msg: msgSvc}
}

// CreateOrder 最为典型的场景，在创建订单之后，要发送一个消息到消息队列上
func (svc *OrderService) CreateOrder(ctx context.Context, sn string) error {
	err := svc.msg.ExecTx(ctx, func(tx *gorm.DB) (msg.Msg, error) {
		now := time.Now().Unix()
		o := &Order{
			SN:    sn,
			Utime: now,
			Ctime: now,
		}
		err := tx.Create(&o).Error
		return msg.Msg{
			// 使用 SN 作为 key，也可以换成 id
			Key:     sn,
			Topic:   "order_created",
			Content: sn,
		}, err
	})
	return err
}

type Order struct {
	Id int64 `gorm:"primaryKey,autoIncrement"`
	// 其它字段都不重要
	// 你愿意加字段就加字段

	// 我这里就懒得加了

	SN    string
	Buyer int64

	Utime int64
	Ctime int64
}
