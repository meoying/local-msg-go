package web

import (
	"github.com/ecodeclub/ginx"
	"github.com/gin-gonic/gin"
)

type Handler struct {
}

func NewHandler() *Handler {
	handler := &Handler{}
	return handler
}

func (handler *Handler) RegisterRoutes(server *gin.Engine) {
	// 全部默认用 default
	server.POST("/", ginx.B(handler.List))
}

func (handler *Handler) List(ctx *ginx.Context, req ListReq) (ginx.Result, error) {
	// 查找数据
}
