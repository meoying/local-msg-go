package lmsg

import (
	"github.com/IBM/sarama"
	"github.com/meoying/local-msg-go/internal/service"
	"github.com/meoying/local-msg-go/internal/sharding"
	"gorm.io/gorm"
)

// NewDefaultService 都是默认配置，所有的本地消息都在一张表里面
func NewDefaultService(
	db *gorm.DB,
	producer sarama.SyncProducer) *service.Service {
	dbs := map[string]*gorm.DB{
		"": db,
	}
	return &service.Service{
		ShardingService: NewDefaultShardingService(dbs, producer,
			sharding.NewNoShard("local_msgs")),
	}
}

// NewDefaultShardingService 创建一个初始化的支持分库分表的 service
func NewDefaultShardingService(dbs map[string]*gorm.DB, producer sarama.SyncProducer, sharding sharding.Sharding) *service.ShardingService {
	return service.NewShardingService(dbs, producer, sharding)
}
