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

func (dao *MsgDAO) Get(ctx context.Context, table string, id int64) (LocalMsg, error) {
	var res LocalMsg
	err := dao.db.WithContext(ctx).Table(table).
		Where("id = ?", id).First(&res).Error
	return res, err
}

func (dao *MsgDAO) List(ctx context.Context, q Query) ([]LocalMsg, error) {
	var res []LocalMsg
	db := dao.db.WithContext(ctx).
		Offset(q.Offset).
		Limit(q.Limit).
		Table(q.Table).Order("id DESC")
	if q.Status >= 0 {
		db = db.Where("status=?", q.Status)
	}
	if q.Key != "" {
		db = db.Where("`key` = ?", q.Key)
	}

	if q.StartTime > 0 {
		db = db.Where("`ctime` >= ?", q.StartTime)
	}

	if q.EndTime > 0 {
		db = db.Where("`ctime` <= ?", q.EndTime)
	}

	err := db.Find(&res).Error
	return res, err
}

func NewMsgDAO(db *gorm.DB) *MsgDAO {
	return &MsgDAO{
		db: db,
	}
}

type Query struct {
	Table  string `json:"table,omitempty"`
	Offset int    `json:"offset,omitempty"`
	Limit  int    `json:"limit,omitempty"`
	Status int8   `json:"status,omitempty"`
	// Key 是精准查询
	Key string `json:"key,omitempty"`

	StartTime int64 `json:"startTime,omitempty"`
	EndTime   int64 `json:"endTime,omitempty"`
}

type LocalMsg struct {
	Id int64 `gorm:"primaryKey,autoIncrement"`

	// Key 是业务的凭证，可以是唯一的，也可以不是唯一的，我们不在意
	Key  string `gorm:"index"`
	Data []byte `gorm:"type:TEXT"`
	// 发送次数，也就是说，立即发送之后，这个计数就会 +1
	SendTimes int

	// 在更新时间和 status 上创建联合索引，
	// 保证在 WHERE 过滤数据的时候，不需要回表
	Status int8 `gorm:"index:utime_status"`
	// 更新时间
	Utime int64 `gorm:"index:utime_status"`
	Ctime int64
}

func (l LocalMsg) TableName() string {
	return "local_msgs"
}

const (
	MsgStatusInit int8 = iota
	MsgStatusSuccess
	MsgStatusFail
)
