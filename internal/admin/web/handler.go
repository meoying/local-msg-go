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

// List 请求，在分库分表的情况下，默认是从名字为空字符串的 DB 中取数据
func (handler *Handler) List(ctx *ginx.Context, req ListReq) (ginx.Result, error) {
	// 查找数据
}
