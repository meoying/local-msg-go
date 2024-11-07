package service

import (
	"context"
	"fmt"
	"github.com/meoying/local-msg-go/internal/dao"
	msg2 "github.com/meoying/local-msg-go/internal/msg"
	"github.com/meoying/local-msg-go/internal/service"
	"github.com/meoying/local-msg-go/internal/sharding"
	"github.com/meoying/local-msg-go/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"testing"
	"time"
)

type LocalServiceTestSuite struct {
	test.BaseSuite
	db00 *gorm.DB
	db01 *gorm.DB
	dbs  map[string]*gorm.DB
}

func (s *LocalServiceTestSuite) SetupSuite() {
	// 要准备好各种数据库
	db00, err := gorm.Open(mysql.Open("root:root@tcp(localhost:13316)/orders_db_00?charset=utf8mb4&collation=utf8mb4_general_ci&parseTime=True&loc=Local&timeout=1s&readTimeout=3s&writeTimeout=3s"))
	require.NoError(s.T(), err)

	db01, err := gorm.Open(mysql.Open("root:root@tcp(localhost:13316)/orders_db_01?charset=utf8mb4&collation=utf8mb4_general_ci&parseTime=True&loc=Local&timeout=1s&readTimeout=3s&writeTimeout=3s"))
	require.NoError(s.T(), err)

	dbs := map[string]*gorm.DB{
		"orders_db_00": db00,
		"orders_db_01": db01,
	}
	s.db00 = db00
	s.db01 = db01
	s.dbs = dbs
}

func (s *LocalServiceTestSuite) TearDownTest() {
	err := s.db00.Exec("TRUNCATE TABLE orders_tab_00").Error
	require.NoError(s.T(), err)
	err = s.db00.Exec("TRUNCATE TABLE orders_tab_01").Error
	require.NoError(s.T(), err)

	err = s.db00.Exec("TRUNCATE TABLE local_msgs_tab_00").Error
	require.NoError(s.T(), err)
	err = s.db00.Exec("TRUNCATE TABLE local_msgs_tab_01").Error
	require.NoError(s.T(), err)

	err = s.db01.Exec("TRUNCATE TABLE orders_tab_00").Error
	require.NoError(s.T(), err)
	err = s.db01.Exec("TRUNCATE TABLE orders_tab_01").Error
	require.NoError(s.T(), err)

	err = s.db01.Exec("TRUNCATE TABLE local_msgs_tab_00").Error
	require.NoError(s.T(), err)
	err = s.db01.Exec("TRUNCATE TABLE local_msgs_tab_01").Error
	require.NoError(s.T(), err)
}

func (s *LocalServiceTestSuite) TestList() {
	testCases := []struct {
		name    string
		biz, db string
		query   Query
		before  func(t *testing.T)

		wantRes []LocalMsg
		wantErr error
	}{
		{
			name: "全部状态",
			before: func(t *testing.T) {
				msgs := make([]dao.LocalMsg, 0, 8)
				msg := s.MockDAOMsg(1, 123)
				msg.Status = 1
				msgs = append(msgs, msg)

				msg = s.MockDAOMsg(2, 123)
				msg.Status = 0
				msgs = append(msgs, msg)
				err := s.db00.Table("local_msgs_tab_00").Create(&msgs).Error
				require.NoError(s.T(), err)
			},
			biz: "test",
			db:  "orders_db_00",
			query: Query{
				Offset: 0,
				Limit:  10,
				Table:  "local_msgs_tab_00",
				// 查询全部状态的数据
				Status: -1,
			},
			wantRes: []LocalMsg{
				s.MockLocalMsg(2, 123, nil),
				s.MockLocalMsg(1, 123, func(msg *LocalMsg) {
					msg.Status = 1
				}),
			},
		},

		{
			name: "根据时间筛查",
			before: func(t *testing.T) {
				msgs := make([]dao.LocalMsg, 0, 8)
				msg := s.MockDAOMsg(1, 123)
				msg.Status = 1
				msgs = append(msgs, msg)

				msg = s.MockDAOMsg(2, 234)
				msg.Status = 0
				msgs = append(msgs, msg)

				msg = s.MockDAOMsg(3, 10086)
				msgs = append(msgs, msg)
				err := s.db00.Table("local_msgs_tab_00").Create(&msgs).Error
				require.NoError(s.T(), err)
			},
			biz: "test",
			db:  "orders_db_00",
			query: Query{
				Offset: 0,
				Limit:  10,
				Table:  "local_msgs_tab_00",
				// 查询全部状态的数据
				Status:    -1,
				StartTime: 300,
			},
			wantRes: []LocalMsg{
				s.MockLocalMsg(3, 10086, nil),
			},
		},
	}

	svc := NewLocalService(nil)
	err := svc.RegisterShardingSvc("test", service.NewShardingService(s.dbs, nil, sharding.Sharding{}))
	require.NoError(s.T(), err)
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			tc.before(t)
			res, err1 := svc.ListMsgs(ctx, tc.biz, tc.db, tc.query)
			assert.ErrorIs(s.T(), err1, tc.wantErr)
			assert.Equal(s.T(), tc.wantRes, res)
		})
		s.TearDownTest()
	}
}

func (s *LocalServiceTestSuite) MockLocalMsg(
	id int64,
	utime int64, cb func(msg *LocalMsg)) LocalMsg {
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
	res := LocalMsg{
		Id:     id,
		Key:    key,
		Msg:    msg,
		Status: dao.MsgStatusInit,
		Utime:  time.UnixMilli(utime),
		Ctime:  time.UnixMilli(utime),
	}
	if cb != nil {
		cb(&res)
	}
	return res
}

func TestLocalService(t *testing.T) {
	suite.Run(t, new(LocalServiceTestSuite))
}
