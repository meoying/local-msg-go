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
	Id   int64  `gorm:"primaryKey,autoIncrement"`
	Data []byte `gorm:"type:BLOB"`
	// 发送次数，也就是说，立即发送之后，这个计数就会 +1
	SendTimes int
	Status    uint8
	// 更新时间
	Utime int64
	Ctime int64
}

const (
	MsgStatusInit    = 0
	MsgStatusSuccess = 1
	MsgStatusFail    = 2
)

// FindSuspendMsg 找到还没有发送成功的消息
func (dao *MsgDAO) FindSuspendMsg(ctx context.Context, offset, limit int) ([]LocalMsg, error) {
	now := time.Now().Unix()
	utime := now - dao.duration.Milliseconds()
	var res []LocalMsg
	err := dao.db.WithContext(ctx).Where("status=? AND utime < ?", utime).
		Limit(limit).Offset(offset).Find(&res).Error
	return res, err
}
