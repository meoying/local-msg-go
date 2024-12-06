package main

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/ecodeclub/ginx/session"
	"github.com/ecodeclub/ginx/session/redis"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	lmsg "github.com/meoying/local-msg-go"
	"github.com/meoying/local-msg-go/internal/service"
	"github.com/meoying/local-msg-go/mockbiz/noshardin_order"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	redis2 "github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"net/http"
	"os"
	"os/signal"
)

// 这个是模拟业务在非分库分表的情况下，引入了依赖之后，直接在本地启动了管理后台的例子
func main() {
	// 初始化prometheus
	initPrometheus()
	// 包含三个步骤：
	// 非分库分表的时候
	// 1. 初始化 lmsg.Service。
	db, err := gorm.Open(mysql.Open("root:root@tcp(localhost:13316)/local_msg_test?charset=utf8mb4&collation=utf8mb4_general_ci&parseTime=True&loc=Local&timeout=1s&readTimeout=3s&writeTimeout=3s"))
	if err != nil {
		panic(err)
	}
	// 这样你能看到输出
	db = db.Debug()
	rdb := redis2.NewClient(&redis2.Options{
		Addr: "localhost:6379",
	})
	mqCfg := sarama.NewConfig()
	mqCfg.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, mqCfg)
	if err != nil {
		panic(err)
	}
	// 初始化上报数据
	msgSvc, err := lmsg.NewDefaultService(db, producer, service.WithMetricExecutor())
	if err != nil {
		panic(err)
	}
	// 在业务中使用的订单服务
	order := noshardin_order.NewOrderService(db, msgSvc)
	println(order)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// 启动补偿任务
		msgSvc.StartAsyncTask(ctx)
		//println(ctx == nil)
	}()

	go func() {
		// 这个步骤是可选的。也就是你的业务可以只使用 msgSvc
		// 而不必启动 adminSvc
		// 当然，你也可以独立部署 admin 服务，
		// 而不必和使用 msgSvc 的业务一起部署
		adminSvc := lmsg.NewAdminLocalService(producer)
		// 如果你有多个业务都是是用了本地消息表，那么这里可以逐个注册
		_ = adminSvc.Register("order", msgSvc)
		// 额外注册一个作为默认的，这一步也可以忽略
		_ = adminSvc.Register("", msgSvc)
		// 这里我没有使用默认的 GIN 的Session 机制，而是使用我自己设计的 Session 机制
		session.SetDefaultProvider(redis.NewSessionProvider(rdb, "test_key"))
		hdl := lmsg.NewAdminHandler(adminSvc)
		server := gin.Default()
		// 跨域
		server.Use(cors.New(cors.Config{
			AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
			AllowHeaders:     []string{"Origin", "Content-Length", "Content-Type", "Authorization"},
			ExposeHeaders:    []string{"Content-Length"},
			AllowCredentials: true,
			// 你在这里可以控制允许的域名
			AllowOriginFunc: func(origin string) bool {
				return true
			},
		}))
		hdl.RegisterRoutes(server)
		// 启动
		server.Run(":8080")
	}()
	// 监听关闭信号，不同操作系统下可能有差异
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, os.Kill)
	// 收到了信号
	<-signalChan
	// 调用 Cancel 就会停止补偿任务
	cancel()
}

func initPrometheus() {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":8081", nil)
	}()
}
