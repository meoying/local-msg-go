package lmsg

import (
	"github.com/IBM/sarama"
	"github.com/meoying/local-msg-go/internal/service"
	"github.com/meoying/local-msg-go/internal/sharding"
	"gorm.io/gorm"
	"log/slog"
	"time"
)

// NewDefaultService 都是默认配置，所有的本地消息都在一张表里面
func NewDefaultService(
	db *gorm.DB,
	producer sarama.SyncProducer) *service.ShardingService {
	dbs := map[string]*gorm.DB{
		"": db,
	}
	return newService(dbs, producer,
		sharding.NewNoShard("local_msgs"))
}

func newService(dbs map[string]*gorm.DB, producer sarama.SyncProducer, sharding sharding.Sharding) *service.ShardingService {
	return service.NewService(dbs, producer, sharding, time.Second*30, 3, slog.Default())
}
