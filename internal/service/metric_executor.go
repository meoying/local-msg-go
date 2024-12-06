package service

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gorm.io/gorm"
	"time"
)

type MetricExecutor struct {
	executor Executor
	TaskExecCounter   *prometheus.CounterVec
	TaskExecDuration  *prometheus.HistogramVec
}

func NewMetricExecutor(executor Executor) *MetricExecutor{
	return &MetricExecutor{
		executor: executor,
		TaskExecCounter: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "local_msg_async_task_exec_total",
				Help: "补偿任务执行次数",
			},
			[]string{"table", "status"}, // 标签:表名和执行状态
		),
		TaskExecDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "local_msg_async_task_exec_duration_seconds",
				Help:    "补偿任务执行时间",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"table"}, // 标签:表名
		),
	}
}

func (m *MetricExecutor) Exec(ctx context.Context, db *gorm.DB, table string) (int, error) {
	start := time.Now()
	cnt,err := m.executor.Exec(ctx,db,table)
	// 记录执行时间
	m.TaskExecDuration.WithLabelValues(table).Observe(time.Since(start).Seconds())
	if err != nil {
		// 记录失败
		m.TaskExecCounter.WithLabelValues(table, "failed").Inc()
	} else {
		// 记录成功
		m.TaskExecCounter.WithLabelValues(table, "success").Inc()
	}
	return cnt,err
}

