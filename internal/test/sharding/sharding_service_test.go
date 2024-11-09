package sharding

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/bwmarrin/snowflake"
	lmsg "github.com/meoying/local-msg-go"
	"github.com/meoying/local-msg-go/internal/dao"
	dlock "github.com/meoying/local-msg-go/internal/lock"
	glock "github.com/meoying/local-msg-go/internal/lock/gorm"
	"github.com/meoying/local-msg-go/internal/sharding"
	"github.com/meoying/local-msg-go/internal/test"
	"github.com/meoying/local-msg-go/internal/test/mocks"
	"github.com/meoying/local-msg-go/mockbiz/sharding_order"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"testing"
	"time"
)

type OrderServiceTestSuite struct {
	db00 *gorm.DB
	db01 *gorm.DB
	dbs  map[string]*gorm.DB
	test.BaseSuite
	rules      sharding.Sharding
	lockClient dlock.Client
}

func (s *OrderServiceTestSuite) SetupSuite() {
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
	lockClient := glock.NewClient(s.db00)
	err = lockClient.InitTable()
	require.NoError(s.T(), err)
	s.lockClient = lockClient
	s.rules = sharding.Sharding{
		ShardingFunc: func(info any) sharding.Dst {
			buyer := info.(int64)
			dbIdx := buyer % 4 / 2
			tabIdx := (buyer % 4) % 2
			return sharding.Dst{
				// 保持和订单表使用同样的分库规则
				DB: fmt.Sprintf("orders_db_%02d", dbIdx),
				// 分表其实很随意的
				Table: fmt.Sprintf("local_msgs_tab_%02d", tabIdx),
			}
		},
		EffectiveTablesFunc: func() []sharding.Dst {
			return []sharding.Dst{
				{
					DB:    "orders_db_00",
					Table: "local_msgs_tab_00",
				},
				{
					DB:    "orders_db_00",
					Table: "local_msgs_tab_01",
				},
				{
					DB:    "orders_db_01",
					Table: "local_msgs_tab_00",
				},
				{
					DB:    "orders_db_01",
					Table: "local_msgs_tab_01",
				},
			}
		},
	}
}

func (s *OrderServiceTestSuite) TearDownTest() {
	err := s.db00.Exec("TRUNCATE TABLE orders_tab_00").Error
	require.NoError(s.T(), err)
	err = s.db00.Exec("TRUNCATE TABLE orders_tab_01").Error
	require.NoError(s.T(), err)

	err = s.db00.Exec("TRUNCATE TABLE local_msgs_tab_00").Error
	require.NoError(s.T(), err)
	err = s.db00.Exec("TRUNCATE TABLE local_msgs_tab_01").Error
	require.NoError(s.T(), err)

	err = s.db00.Exec("TRUNCATE TABLE distributed_locks").Error
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

func (s *OrderServiceTestSuite) TestCreateOrder() {
	rules := s.rules
	testCases := []struct {
		name string

		mock func(ctrl *gomock.Controller) sarama.SyncProducer
		// 验证数据
		after func(t *testing.T)

		// 输入
		sn    string
		buyer int64

		wantErr error
	}{
		{
			name: "直接发送成功",
			mock: func(ctrl *gomock.Controller) sarama.SyncProducer {
				producer := mocks.NewMockSyncProducer(ctrl)
				producer.EXPECT().SendMessage(gomock.Any()).Return(1, 1, nil)
				return producer
			},
			after: func(t *testing.T) {
				// 查找 local msgs 里面的数据，应该是发送成功的状态
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				var dmsg dao.LocalMsg
				dst := rules.ShardingFunc(int64(1))
				err := s.db00.WithContext(ctx).
					Table(dst.Table).
					Where("`key`= ?", "case1").First(&dmsg).Error
				assert.NoError(t, err)
				assert.True(t, len(dmsg.Data) > 0)
				assert.Equal(t, dmsg.SendTimes, 1)
				assert.Equal(t, dmsg.Status, dao.MsgStatusSuccess)
			},
			sn:    "case1",
			buyer: 1,
		},

		{
			name: "发送消息失败",
			mock: func(ctrl *gomock.Controller) sarama.SyncProducer {
				producer := mocks.NewMockSyncProducer(ctrl)
				producer.EXPECT().SendMessage(gomock.Any()).
					Return(0, 0, errors.New("mock error"))
				return producer
			},
			after: func(t *testing.T) {
				// 查找 local msgs 里面的数据，应该是发送成功的状态
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				var dmsg dao.LocalMsg
				dst := rules.ShardingFunc(int64(1))
				err := s.db00.WithContext(ctx).
					Table(dst.Table).
					Where("`key`= ?", "case2").First(&dmsg).Error
				assert.NoError(t, err)
				assert.True(t, len(dmsg.Data) > 0)
				assert.Equal(t, dmsg.SendTimes, 1)
				assert.Equal(t, dmsg.Status, dao.MsgStatusInit)
			},
			sn:    "case2",
			buyer: 1,
		},
	}

	node, _ := snowflake.NewNode(1)

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			producer := tc.mock(ctrl)
			msgSvc := lmsg.NewDefaultShardingService(s.dbs, producer, s.lockClient, rules)
			svc := sharding_order.NewOrderService(node, msgSvc)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()
			err := svc.CreateOrder(ctx, tc.sn, tc.buyer)
			assert.Equal(t, tc.wantErr, err)
			tc.after(t)
		})
	}
}

func (s *OrderServiceTestSuite) TestAsyncTask() {
	// 这里我们的测试逻辑很简单，就是在数据库中直接插入不同数据
	// 模拟四条，来覆盖不同的场景
	msgs := make([]dao.LocalMsg, 0, 4)
	now := time.Now().UnixMilli()
	// id = 1 的会取出来
	msg1 := s.MockDAOMsg(1, now-(time.Second*11).Milliseconds())
	msgs = append(msgs, msg1)

	// id = 2 的不会取出来，因为已经彻底失败了
	msg2 := s.MockDAOMsg(2, now-(time.Second*11).Milliseconds())
	msg2.Status = dao.MsgStatusFail
	msgs = append(msgs, msg2)

	// id = 3 的不会取出来，因为已经成功了
	msg3 := s.MockDAOMsg(3, now-(time.Second*11).Milliseconds())
	msg3.Status = dao.MsgStatusSuccess
	msgs = append(msgs, msg3)

	// id = 4 的不会取出来，因为还没到时间间隔，业务可能还在处理中
	msg4 := s.MockDAOMsg(4, now-(time.Second*1).Milliseconds())
	msg4.Status = dao.MsgStatusInit
	msgs = append(msgs, msg4)

	// id = 5 会取出来，但是它只有最后一次发送机会了，发送会成功
	msg5 := s.MockDAOMsg(5, now-(time.Second*13).Milliseconds())
	msg5.Status = dao.MsgStatusInit
	msg5.SendTimes = 2
	msgs = append(msgs, msg5)

	// id = 6 会取出来，但是它只有最后一次发送机会了，发送会失败
	msg6 := s.MockDAOMsg(6, now-(time.Second*13).Milliseconds())
	msg6.Status = dao.MsgStatusInit
	msg6.SendTimes = 2

	msgs = append(msgs, msg6)
	// 所有的数据库，所有的表都执行一遍
	for _, dst := range s.rules.EffectiveTablesFunc() {
		db := s.dbs[dst.DB]
		err := db.Table(dst.Table).Create(&msgs).Error
		require.NoError(s.T(), err)
	}

	ctrl := gomock.NewController(s.T())
	defer ctrl.Finish()
	producer := mocks.NewMockSyncProducer(ctrl)
	producer.EXPECT().SendMessage(gomock.Any()).DoAndReturn(func(pmsg *sarama.ProducerMessage) (int32, int64, error) {
		data := []byte(pmsg.Key.(sarama.StringEncoder))
		if bytes.Contains(data, []byte("success")) {
			return 1, 1, nil
		}
		return 0, 0, errors.New("mock error")
	}).AnyTimes()

	svc := lmsg.NewDefaultShardingService(s.dbs,
		producer,
		s.lockClient,
		s.rules)
	svc.WaitDuration = time.Second * 10
	svc.MaxTimes = 3
	// 两条一批，减少构造数据
	svc.BatchSize = 2

	// 五秒钟可以确保所有的数据都处理完，但是 msg5 时间还不到
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	svc.StartAsyncTask(ctx)
	// 等过期
	<-ctx.Done()

	// 如果要是到了这里，那么预期中应该所有的数据都要么发送完了，要么已经超过发送次数了

	for _, dst := range s.rules.EffectiveTablesFunc() {
		newCtx, newCancel := context.WithTimeout(context.Background(), time.Second*3)
		msgs = []dao.LocalMsg{}
		db := s.dbs[dst.DB]
		err := db.WithContext(newCtx).
			Table(dst.Table).Order("id ASC").Find(&msgs).Error
		newCancel()
		assert.NoError(s.T(), err)
		// 开始断言
		// id 为 1
		msg1.Status = dao.MsgStatusSuccess
		msg1.SendTimes = 1
		s.AssertMsg(msg1, msgs[0])

		// id = 2 不会变
		s.AssertMsg(msg2, msgs[1])
		// id = 3 的不会变
		s.AssertMsg(msg3, msgs[2])
		// id = 4 的不会变
		s.AssertMsg(msg4, msgs[3])
		// id = 5
		msg5.Status = dao.MsgStatusSuccess
		msg5.SendTimes = 3
		s.AssertMsg(msg5, msgs[4])
		// id = 6
		msg6.Status = dao.MsgStatusFail
		msg6.SendTimes = 3
		s.AssertMsg(msg6, msgs[5])
	}
}

func TestShardingService(t *testing.T) {
	suite.Run(t, new(OrderServiceTestSuite))
}
