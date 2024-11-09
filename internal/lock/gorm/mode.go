package glock

const (
	// ModeInsertFirst 是指默认要加的分布式锁大概率还没有人加过
	// 比如说针对某个订单加锁，那么很显然这个时候订单刚创建，有谁会去加锁呢？
	// 采用这两种 Mode 就是为了在不同的场景下追求极致的性能
	// 如果你预测你的 key 大概率不在数据库中，就用这个模式
	ModeInsertFirst = "insert"
	// ModeCASFirst 是指默认要加的分布式锁已经很多人加过之后又释放了
	// 也就是大多数时候，数据库里面已经有这个 key 对应的数据了
	// 如果你预测你的 key 大概率已经在数据库中了，就用这个模式
	ModeCASFirst = "cas"

	// 理论上还有一种 SELECT FOR UPDATE 的写法，但是我们没有必要支持
	// 它的性能极差，并且容易引发表锁、死锁之类的问题
)
