package lmsg

import (
	"github.com/IBM/sarama"
	service2 "github.com/meoying/local-msg-go/internal/admin/service"
	"github.com/meoying/local-msg-go/internal/admin/web"
)

type AdminHandler = web.Handler

func NewAdminHandler(svc *service2.LocalService) *AdminHandler {
	return web.NewHandler(svc)
}

func NewAdminLocalService(producer sarama.SyncProducer) *service2.LocalService {
	return service2.NewLocalService(producer)
}
