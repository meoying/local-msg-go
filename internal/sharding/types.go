package sharding

import (
	"fmt"
	"github.com/meoying/local-msg-go/internal/msg"
)

// Sharding 如果你有分库分表，那么就需要提供这个配置
type Sharding struct {
	// 返回分库和分表的信息
	ShardingFunc func(msg msg.Msg) Dst
	// 有效的目标表，例如说在按照日期分库分表之后，
	// 那么老的日期用的表就已经用不上了
	// 定时任务会迭代所有的表，而后找出发送失败的消息，进行补发
	EffectiveTablesFunc func() []Dst
}

type Dst struct {
	// 分库，我们会利用这个信息来获得连接信息
	DB string
	// 分表
	Table string
}

func (d Dst) TableName() string {
	if d.DB == "" {
		return d.Table
	}
	return fmt.Sprintf("%s.%s", d.DB, d.Table)
}
