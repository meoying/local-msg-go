package sharding

import (
	"context"
	"fmt"
	"github.com/bwmarrin/snowflake"
	"github.com/meoying/local-msg-go/internal/msg"
	"github.com/meoying/local-msg-go/internal/service"
	"gorm.io/gorm"
	"time"
)

// OrderService 模拟订单服务
// 这里假设分库分表是按照买家 id 分成了 2 * 2 四个表，
type OrderService struct {
	msg      *service.ShardingService
	snowNode *snowflake.Node
}

func NewOrderService(node *snowflake.Node,
	msgSvc *service.ShardingService) *OrderService {
	return &OrderService{
		snowNode: node,
		msg:      msgSvc,
	}
}

// CreateOrder 最为典型的场景，在创建订单之后，要发送一个消息到消息队列上
func (svc *OrderService) CreateOrder(
	ctx context.Context,
	sn string,
	buyer int64) error {
	err := svc.msg.ExecTx(ctx, buyer, func(tx *gorm.DB) (msg.Msg, error) {
		now := time.Now().UnixMilli()
		// 要按照分库分表的规则，推断出来表
		// 在使用分库分表的情况下，只要求本地消息表和订单表在同一个库
		// 不要求使用同样的分表规则
		tableName := fmt.Sprintf("orders_tab_%02d", (buyer%4)%2)
		err := tx.Table(tableName).Create(&Order{
			// 一般来说，ID 都得靠雪花算法来生成，当然你也可以换成别的。
			Id:    svc.snowNode.Generate().Int64(),
			SN:    sn,
			Buyer: buyer,
			Utime: now,
			Ctime: now,
		}).Error
		if err != nil {
			return msg.Msg{}, err
		}
		return msg.Msg{
			Key:   sn,
			Topic: "order_created",
			// 例如时候，要是有必要可以把 ID 传递过去
			Content: []byte(sn),
		}, nil
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
