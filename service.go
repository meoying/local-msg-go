package lmsg

import (
	"github.com/IBM/sarama"
	dlock "github.com/meoying/local-msg-go/internal/lock"
	glock "github.com/meoying/local-msg-go/internal/lock/gorm"
	"github.com/meoying/local-msg-go/internal/service"
	"github.com/meoying/local-msg-go/internal/sharding"
	"gorm.io/gorm"
)

// NewDefaultService 都是默认配置，所有的本地消息都在一张表里面
// 在调度的时候，会使用一张表来实现分布式锁
func NewDefaultService(
	db *gorm.DB,
	producer sarama.SyncProducer,
	opts ...service.ShardingServiceOpt,
	) (*service.Service, error) {
	dbs := map[string]*gorm.DB{
		"": db,
	}
	lockClient := glock.NewClient(db)
	err := lockClient.InitTable()
	if err != nil {
		return nil, err
	}
	return &service.Service{
		ShardingService: NewDefaultShardingService(dbs, producer,
			glock.NewClient(db),
			sharding.NewNoShard("local_msgs"),opts...),
	}, nil
}

// NewDefaultShardingService 创建一个初始化的支持分库分表的 service
func NewDefaultShardingService(dbs map[string]*gorm.DB,
	producer sarama.SyncProducer,
	lockClient dlock.Client,
	sharding sharding.Sharding,opts ...service.ShardingServiceOpt) *service.ShardingService {
	return service.NewShardingService(dbs, producer, lockClient, sharding,opts...)
}


