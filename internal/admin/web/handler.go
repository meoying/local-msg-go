package web

import (
	"github.com/ecodeclub/ekit/slice"
	"github.com/ecodeclub/ginx"
	"github.com/gin-gonic/gin"
	service2 "github.com/meoying/local-msg-go/internal/admin/service"
)

type Handler struct {
	svc *service2.LocalService
}

func NewHandler(svc *service2.LocalService) *Handler {
	handler := &Handler{
		svc: svc,
	}
	return handler
}

func (handler *Handler) RegisterRoutes(server *gin.Engine) {
	server.POST("/local_msg/list", ginx.B(handler.List))
	server.POST("/local_msg/retry", ginx.B(handler.Retry))
}

// List 请求，在分库分表的情况下，默认是从名字为空字符串的 DB 中取数据
func (handler *Handler) List(ctx *ginx.Context, req ListReq) (ginx.Result, error) {
	// 查找数据
	res, err := handler.svc.ListMsgs(ctx, req.Biz, req.DB, req.Query)
	if err != nil {
		return ginx.Result{}, err
	}
	return ginx.Result{
		Data: slice.Map(res, func(idx int, src service2.LocalMsg) LocalMsg {
			return newLocalMsg(req.Biz, req.DB, req.Query.Table, src)
		}),
	}, nil
}

func (handler *Handler) Retry(ctx *ginx.Context, req RetryReq) (ginx.Result, error) {
	err := handler.svc.Retry(ctx, req.Biz, req.DB, req.Table, req.Id)
	if err != nil {
		return ginx.Result{}, err
	}
	return ginx.Result{}, nil
}
