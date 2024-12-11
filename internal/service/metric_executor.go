package service

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gorm.io/gorm"
	"strconv"
	"time"
)

type MetricExecutor struct {
	executor         Executor
	TaskExecDuration *prometheus.HistogramVec
}

func NewMetricExecutor(executor Executor) *MetricExecutor {
	return &MetricExecutor{
		executor: executor,
		TaskExecDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "local_msg_async_task_exec_duration_seconds",
				Help:    "补偿任务执行时间",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"table", "success"}, // 标签:表名
		),
	}
}

func (m *MetricExecutor) Exec(ctx context.Context, db *gorm.DB, table string) (int, error) {
	start := time.Now()
	cnt, err := m.executor.Exec(ctx, db, table)
	// 记录执行时间
	m.TaskExecDuration.WithLabelValues(table, strconv.FormatBool(err == nil)).Observe(time.Since(start).Seconds())
	return cnt, err
}
