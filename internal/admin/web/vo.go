package web

type Msg struct {
	Id    int64
	Biz   string
	Topic string
	// Partition 是指
	Partition int64
	Content   string
}

// ListReq 罗列出来的状态
// 不支持任意检索
type ListReq struct {
	// 查询的业务
	Biz string

	// key 是精确匹配
	Key string
	// 检索的时间段
	StartTime int64
	EndTime   int64

	// 分页信息
	Offset int
	Limit  int
}
