package sharding

// NewNoShard 指定表名
func NewNoShard(table string) Sharding {
	return Sharding{
		ShardingFunc: func(msg any) Dst {
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
