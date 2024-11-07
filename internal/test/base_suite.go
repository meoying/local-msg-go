package test

import (
	"encoding/json"
	"fmt"
	"github.com/meoying/local-msg-go/internal/dao"
	msg2 "github.com/meoying/local-msg-go/internal/msg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type BaseSuite struct {
	suite.Suite
}

func (s *BaseSuite) AssertMsg(
	expect dao.LocalMsg,
	actual dao.LocalMsg) {
	expect.Utime = 0
	expect.Ctime = 0
	actual.Utime = 0
	actual.Ctime = 0
	assert.Equal(s.T(), expect, actual)
}

func (s *BaseSuite) MockDAOMsg(id int64, utime int64) dao.LocalMsg {
	var key string
	if id%2 == 1 {
		key = fmt.Sprintf("%d_success", id)
	} else {
		key = fmt.Sprintf("%d_fail", id)
	}

	msg := msg2.Msg{
		Key:     key,
		Topic:   "order_created",
		Content: []byte("这是内容"),
	}
	val, _ := json.Marshal(msg)
	return dao.LocalMsg{
		Id:     id,
		Key:    key,
		Data:   val,
		Status: dao.MsgStatusInit,
		Utime:  utime,
		Ctime:  utime,
	}
}
