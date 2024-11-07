package sharding

// Sharding 如果你有分库分表，那么就需要提供这个配置
type Sharding struct {
	// 返回分库和分表的信息
	// info 是你用来分库分表的信息，这取决于你的业务是如何执行的
	ShardingFunc func(info any) Dst
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
