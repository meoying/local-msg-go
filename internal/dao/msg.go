package dao

import (
	"context"
	"gorm.io/gorm"
	"time"
)

type MsgDAO struct {
	db *gorm.DB
	// 最大初始化时间，也就是说超过这个，我们就认为消息是发送失败了
	// 补偿任务就会启动补偿
	duration time.Duration
}

type LocalMsg struct {
	Id int64 `gorm:"primaryKey,autoIncrement"`

	// Key 是业务的凭证，可以是唯一的，也可以不是唯一的，我们不在意
	Key  string `gorm:"index"`
	Data []byte `gorm:"type:BLOB"`
	// 发送次数，也就是说，立即发送之后，这个计数就会 +1
	SendTimes int

	// 在更新时间和 status 上创建联合索引，
	// 保证在 WHERE 过滤数据的时候，不需要回表
	Status uint8 `gorm:"index:utime_status"`
	// 更新时间
	Utime int64 `gorm:"index:utime_status"`
	Ctime int64
}

func (l LocalMsg) TableName() string {
	return "local_msgs"
}

const (
	MsgStatusInit uint8 = iota
	MsgStatusSuccess
	MsgStatusFail
)

// FindSuspendMsg 找到还没有发送成功的消息
func (dao *MsgDAO) FindSuspendMsg(ctx context.Context, offset, limit int) ([]LocalMsg, error) {
	now := time.Now().Unix()
	utime := now - dao.duration.Milliseconds()
	var res []LocalMsg
	err := dao.db.WithContext(ctx).Where("status=? AND utime < ?", MsgStatusInit, utime).
		Limit(limit).Offset(offset).Find(&res).Error
	return res, err
}
