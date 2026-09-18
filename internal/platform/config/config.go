// Package config 从环境变量装载运行配置。
//
// 用环境变量而非配置文件，是为了让同一个二进制在 NAS、容器、systemd 下
// 都不需要额外分发文件。车道拓扑是唯一复杂到需要结构化的部分，用 JSON 表达。
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/prohan-git/CleanArchitectureServe/internal/domain/scheduling"
)

type Config struct {
	// DBPath 是调度库路径。它与业务库是两个独立文件：
	// 调度可以先独立演进（将来整体换 PostgreSQL），业务库不必跟着动。
	DBPath string

	HTTPAddr        string
	ShutdownTimeout time.Duration

	// WorkerID 标识 worker 实例，写进租约。多实例部署时必须各不相同。
	WorkerID string
	// PollInterval 是车道空闲时的轮询间隔。
	// 这就是不引入 Redis 所付出的全部代价：最坏情况下任务延迟这么久才开始跑。
	PollInterval time.Duration
	// LeaseDuration 必须显著大于任务的正常执行时长，否则任务会被误判为崩溃而重复派发。
	LeaseDuration time.Duration
	// ReapInterval 是回收过期租约的扫描周期。
	ReapInterval time.Duration

	Lanes *scheduling.Topology

	// InferenceEndpoint 指向独立的推理服务（Python）。留空则使用 demo 执行器。
	InferenceEndpoint string
	InferenceTimeout  time.Duration
}

// laneSpec 是车道在配置里的 JSON 形态。
type laneSpec struct {
	Name     string  `json:"name"`
	Kind     string  `json:"kind"`
	Capacity int     `json:"capacity"`
	RatePerS float64 `json:"rate_per_s"`
}

// defaultLanes 是一套覆盖典型资源类型的默认拓扑。
// gpu 串行对应 CT 独占 / GPU 批量推理；cpu 池对应缩略图；api 限速对应调外部服务。
const defaultLanes = `[
  {"name":"gpu","kind":"serial"},
  {"name":"cpu","kind":"pool","capacity":4},
  {"name":"io","kind":"pool","capacity":8},
  {"name":"api","kind":"rate_limited","capacity":2,"rate_per_s":5}
]`

func Load() (*Config, error) {
	cfg := &Config{
		DBPath:            env("SCHED_DB_PATH", "./data/scheduler.db"),
		HTTPAddr:          env("SCHED_HTTP_ADDR", ":8080"),
		ShutdownTimeout:   envDuration("SCHED_SHUTDOWN_TIMEOUT", 15*time.Second),
		WorkerID:          env("SCHED_WORKER_ID", defaultWorkerID()),
		PollInterval:      envDuration("SCHED_POLL_INTERVAL", time.Second),
		LeaseDuration:     envDuration("SCHED_LEASE_DURATION", 5*time.Minute),
		ReapInterval:      envDuration("SCHED_REAP_INTERVAL", 30*time.Second),
		InferenceEndpoint: env("SCHED_INFERENCE_ENDPOINT", ""),
		InferenceTimeout:  envDuration("SCHED_INFERENCE_TIMEOUT", 2*time.Minute),
	}

	var specs []laneSpec
	if err := json.Unmarshal([]byte(env("SCHED_LANES", defaultLanes)), &specs); err != nil {
		return nil, fmt.Errorf("parse SCHED_LANES: %w", err)
	}
	lanes := make([]scheduling.Lane, 0, len(specs))
	for _, s := range specs {
		l, err := scheduling.NewLane(s.Name, scheduling.LaneKind(s.Kind), s.Capacity, s.RatePerS)
		if err != nil {
			return nil, fmt.Errorf("lane %q: %w", s.Name, err)
		}
		lanes = append(lanes, l)
	}
	topo, err := scheduling.NewTopology(lanes...)
	if err != nil {
		return nil, err
	}
	cfg.Lanes = topo

	if cfg.InferenceTimeout >= cfg.LeaseDuration {
		return nil, fmt.Errorf("SCHED_INFERENCE_TIMEOUT (%s) must be shorter than SCHED_LEASE_DURATION (%s), "+
			"otherwise the lease expires while the task is still running and the job gets dispatched twice",
			cfg.InferenceTimeout, cfg.LeaseDuration)
	}
	return cfg, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "worker"
	}
	return host + "-" + strconv.Itoa(os.Getpid())
}
