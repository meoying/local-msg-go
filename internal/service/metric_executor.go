package service

import (
	"context"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gorm.io/gorm"
)

type MetricExecutor struct {
	executor         Executor
	TaskExecDuration *prometheus.HistogramVec
	RetryFailCounter *prometheus.CounterVec
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
		RetryFailCounter: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "compensation_task_retry_failures_total",
				Help: "补偿任务重试多次后仍然失败的总次数",
			},
			[]string{"table"}, // 标签: 表名
		),
	}
}

func (m *MetricExecutor) Exec(ctx context.Context, db *gorm.DB, table string) (int, int, error) {
	start := time.Now()
	cnt, errCount, err := m.executor.Exec(ctx, db, table)
	// 记录最终失败的次数
	if err != nil && errCount> 0 {
		m.RetryFailCounter.WithLabelValues(table).Add(float64(errCount))
	}
	// 记录执行时间
	m.TaskExecDuration.WithLabelValues(table, strconv.FormatBool(err == nil)).Observe(time.Since(start).Seconds())
	return cnt,errCount, err
}
