package web

import (
	"github.com/meoying/local-msg-go/internal/admin/service"
	"github.com/meoying/local-msg-go/internal/msg"
)

type LocalMsg struct {
	Biz   string `json:"biz,omitempty"`
	DB    string `json:"db,omitempty"`
	Table string `json:"table,omitempty"`

	Id        int64   `json:"id,omitempty"`
	Msg       msg.Msg `json:"msg"`
	Key       string  `json:"key,omitempty"`
	Status    int8    `json:"status,omitempty"`
	SendTimes int     `json:"sendTimes,omitempty"`
	Ctime     int64   `json:"ctime,omitempty"`
	Utime     int64   `json:"utime,omitempty"`
}

func newLocalMsg(biz, db, table string, msg service.LocalMsg) LocalMsg {
	return LocalMsg{
		Biz:       biz,
		DB:        db,
		Table:     table,
		Id:        msg.Id,
		Msg:       msg.Msg,
		Key:       msg.Key,
		Status:    msg.Status,
		SendTimes: msg.SendTimes,
		Ctime:     msg.Ctime.UnixMilli(),
		Utime:     msg.Utime.UnixMilli(),
	}
}

// ListReq 罗列出来的状态
// 不支持任意检索
type ListReq struct {
	Biz   string `json:"biz"`
	DB    string `json:"db"`
	Query Query  `json:"query"`
}

type RetryReq struct {
	Biz   string `json:"biz"`
	DB    string `json:"db"`
	Table string `json:"table"`
	Id    int64  `json:"id"`
}

type Query = service.Query
