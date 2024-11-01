package sharding

import (
	"github.com/meoying/local-msg-go/internal/msg"
)

// NewNoShard 指定表名
func NewNoShard(table string) Sharding {
	return Sharding{
		ShardingFunc: func(msg msg.Msg) Dst {
			return Dst{
				Table: table,
			}
		},
		EffectiveTablesFunc: func() []Dst {
			return []Dst{
				{
					Table: table,
				},
			}
		},
	}
}
