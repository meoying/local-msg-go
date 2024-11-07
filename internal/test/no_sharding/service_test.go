package no_sharding

import (
	"bytes"
	"context"
	"errors"
	"github.com/IBM/sarama"
	lmsg "github.com/meoying/local-msg-go"
	"github.com/meoying/local-msg-go/internal/dao"
	"github.com/meoying/local-msg-go/internal/test"
	"github.com/meoying/local-msg-go/internal/test/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"testing"
	"time"
)

// OrderServiceTestSuite 没有分库分表的测试
// 因为 Kafka 消息发送成功与否完全不可控，所以这里用 mock 接口来替代
type OrderServiceTestSuite struct {
	db *gorm.DB
	test.BaseSuite
}

func TestService(t *testing.T) {
	suite.Run(t, new(OrderServiceTestSuite))
}

func (s *OrderServiceTestSuite) SetupSuite() {
	db, err := gorm.Open(mysql.Open("root:root@tcp(localhost:13316)/local_msg_test?charset=utf8mb4&collation=utf8mb4_general_ci&parseTime=True&loc=Local&timeout=1s&readTimeout=3s&writeTimeout=3s"))
	require.NoError(s.T(), err)
	// 先把表建好
	err = db.AutoMigrate(&dao.LocalMsg{}, &Order{})
	require.NoError(s.T(), err)
	s.db = db
}

func (s *OrderServiceTestSuite) TearDownTest() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := s.db.WithContext(ctx).Exec("TRUNCATE TABLE orders;").Error
	assert.NoError(s.T(), err)
	err = s.db.WithContext(ctx).Exec("TRUNCATE TABLE local_msgs;").Error
	assert.NoError(s.T(), err)
}

// 测试补偿任务
func (s *OrderServiceTestSuite) TestAsyncTask() {
	// 这里我们的测试逻辑很简单，就是在数据库中直接插入不同数据
	// 模拟四条，来覆盖不同的场景
	msgs := make([]dao.LocalMsg, 0, 4)
	now := time.Now().UnixMilli()
	// id = 1 的会取出来
	msg1 := s.MockLocalMsg(1, now-(time.Second*11).Milliseconds())
	msgs = append(msgs, msg1)

	// id = 2 的不会取出来，因为已经彻底失败了
	msg2 := s.MockLocalMsg(2, now-(time.Second*11).Milliseconds())
	msg2.Status = dao.MsgStatusFail
	msgs = append(msgs, msg2)

	// id = 3 的不会取出来，因为已经成功了
	msg3 := s.MockLocalMsg(3, now-(time.Second*11).Milliseconds())
	msg3.Status = dao.MsgStatusSuccess
	msgs = append(msgs, msg3)

	// id = 4 的不会取出来，因为还没到时间间隔，业务可能还在处理中
	msg4 := s.MockLocalMsg(4, now-(time.Second*1).Milliseconds())
	msg4.Status = dao.MsgStatusInit
	msgs = append(msgs, msg4)

	// id = 5 会取出来，但是它只有最后一次发送机会了，发送会成功
	msg5 := s.MockLocalMsg(5, now-(time.Second*13).Milliseconds())
	msg5.Status = dao.MsgStatusInit
	msg5.SendTimes = 2
	msgs = append(msgs, msg5)

	// id = 6 会取出来，但是它只有最后一次发送机会了，发送会失败
	msg6 := s.MockLocalMsg(6, now-(time.Second*13).Milliseconds())
	msg6.Status = dao.MsgStatusInit
	msg6.SendTimes = 2

	msgs = append(msgs, msg6)
	err := s.db.Create(&msgs).Error
	require.NoError(s.T(), err)
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

	svc := lmsg.NewDefaultService(s.db, producer)
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
	newCtx, newCancel := context.WithTimeout(context.Background(), time.Second*3)
	defer newCancel()
	msgs = []dao.LocalMsg{}
	err = s.db.WithContext(newCtx).Order("id ASC").Find(&msgs).Error
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

func (s *OrderServiceTestSuite) TestCreateOrder() {
	testCases := []struct {
		name string
		mock func(ctrl *gomock.Controller) sarama.SyncProducer
		// 验证数据
		after func(t *testing.T)

		// 输入
		sn string

		wantErr error
	}{
		{
			// 在本地业务成功之后，立刻发送成功了
			name: "创建成功，消息发送成功",
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
				err := s.db.WithContext(ctx).
					Where("`key`= ?", "case1").First(&dmsg).Error
				assert.NoError(t, err)
				assert.True(t, len(dmsg.Data) > 0)
				assert.Equal(t, dmsg.SendTimes, 1)
				assert.Equal(t, dmsg.Status, dao.MsgStatusSuccess)
			},
			sn: "case1",
		},
		{
			name: "创建成功，消息发送失败",
			mock: func(ctrl *gomock.Controller) sarama.SyncProducer {
				producer := mocks.NewMockSyncProducer(ctrl)
				producer.EXPECT().SendMessage(gomock.Any()).
					// 这个错误，CreateOrder 是拿不到的
					Return(1, 1, errors.New("abc"))
				return producer
			},
			after: func(t *testing.T) {
				// 查找 local msgs 里面的数据，应该是发送成功的状态
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				var dmsg dao.LocalMsg
				err := s.db.WithContext(ctx).
					Where("`key` = ?", "case2").First(&dmsg).Error
				assert.NoError(t, err)
				assert.True(t, len(dmsg.Data) > 0)
				assert.Equal(t, dmsg.SendTimes, 1)
				// 没有发成功，所以还是这个错误
				assert.Equal(t, dmsg.Status, dao.MsgStatusInit)
			},
			sn: "case2",
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			producer := tc.mock(ctrl)
			msgSvc := lmsg.NewDefaultService(s.db, producer)
			svc := NewOrderService(s.db, msgSvc)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()
			err := svc.CreateOrder(ctx, tc.sn)
			assert.Equal(t, tc.wantErr, err)
			tc.after(t)
		})
	}
}
