package scheduling

import "fmt"

// LaneKind 描述一条车道的资源语义。
//
// 车道建模的是"资源"，不是"队列名"。CT 机串行独占是因为世界上只有一台 CT，
// 而不是因为我们给它起了个队列名。把它建成资源，容量才能配置、才能被调度器
// 全局保证；建成队列名，就只能靠"记得给 worker 加 --concurrency=1"来保证。
type LaneKind string

const (
	// LaneSerial 串行独占：同一时刻全局只允许一个任务在跑（CT 机、GPU 独占推理）。
	LaneSerial LaneKind = "serial"
	// LanePool 并发池：允许 Capacity 个任务同时跑（血常规的多名护士、CPU 缩略图）。
	LanePool LaneKind = "pool"
	// LaneRateLimited 限速：并发之外还受每秒速率约束（调用外部 API）。
	LaneRateLimited LaneKind = "rate_limited"
)

// Lane 是一类可被争用的执行资源。
type Lane struct {
	Name     string
	Kind     LaneKind
	Capacity int     // 允许的最大在途任务数；Serial 恒为 1
	RatePerS float64 // 仅 LaneRateLimited 有意义，每秒允许启动的任务数
}

// NewLane 构造并校验一条车道。
func NewLane(name string, kind LaneKind, capacity int, ratePerS float64) (Lane, error) {
	if name == "" {
		return Lane{}, fmt.Errorf("%w: lane name is empty", ErrInvalidArgument)
	}
	switch kind {
	case LaneSerial:
		capacity = 1
	case LanePool:
		if capacity < 1 {
			return Lane{}, fmt.Errorf("%w: pool lane %q needs capacity >= 1", ErrInvalidArgument, name)
		}
	case LaneRateLimited:
		if capacity < 1 {
			return Lane{}, fmt.Errorf("%w: rate_limited lane %q needs capacity >= 1", ErrInvalidArgument, name)
		}
		if ratePerS <= 0 {
			return Lane{}, fmt.Errorf("%w: rate_limited lane %q needs rate > 0", ErrInvalidArgument, name)
		}
	default:
		return Lane{}, fmt.Errorf("%w: unknown lane kind %q", ErrInvalidArgument, kind)
	}
	return Lane{Name: name, Kind: kind, Capacity: capacity, RatePerS: ratePerS}, nil
}

// Topology 是运行时全部车道的集合，由配置驱动。
type Topology struct {
	lanes map[string]Lane
}

func NewTopology(lanes ...Lane) (*Topology, error) {
	t := &Topology{lanes: make(map[string]Lane, len(lanes))}
	for _, l := range lanes {
		if _, dup := t.lanes[l.Name]; dup {
			return nil, fmt.Errorf("%w: duplicate lane %q", ErrInvalidArgument, l.Name)
		}
		t.lanes[l.Name] = l
	}
	return t, nil
}

func (t *Topology) Lookup(name string) (Lane, bool) {
	l, ok := t.lanes[name]
	return l, ok
}

func (t *Topology) All() []Lane {
	out := make([]Lane, 0, len(t.lanes))
	for _, l := range t.lanes {
		out = append(out, l)
	}
	return out
}
