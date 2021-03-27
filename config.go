package timingwheel

import "fmt"

const (
	// 时间轮单圈容量
	timingWheelCapacityDefault = 60
	timingWheelCapacityMin     = 10
	timingWheelCapacityMax     = 100

	// 时间轮刻度（毫秒）
	timingWheelTickDefault = 100
	timingWheelTickMin     = 10
	timingWheelTickMax     = 200

	// 并发任务数量
	timersDefault = 10
	timersMin     = 5
	timersMax     = 20
)

// Config 时间轮设置项
type Config struct {
	// 时间轮单圈容量
	Capacity int `yaml,json:"capacity"`

	// 时间轮调用间隔
	Tick int64 `yaml,json:"tick"`

	// 并行定时器数量
	Timers int `yaml,josn:"timers"`
}

// DefaultConfig 默认时间轮设置
func DefaultConfig() *Config {
	return &Config{
		Capacity: timingWheelCapacityDefault,
		Tick:     timingWheelTickDefault,
		Timers:   timersDefault,
	}
}

// NewConfig 新建时间轮设置
func NewConfig(cap int, ti int64, timers int) *Config {
	return &Config{
		Capacity: cap,
		Tick:     ti,
		Timers:   timers,
	}
}

// Check 设置项检查
func (c *Config) Check() {
	if c.Capacity < timingWheelCapacityMin {
		c.Capacity = timingWheelCapacityMin
	}
	if c.Capacity > timingWheelCapacityMax {
		c.Capacity = timingWheelCapacityMax
	}

	if c.Tick < timingWheelTickMin {
		c.Tick = timingWheelTickMin
	}
	if c.Tick > timingWheelTickMax {
		c.Tick = timingWheelTickMax
	}

	if c.Timers < timersMin {
		c.Timers = timersMin
	}
	if c.Timers > timersMax {
		c.Timers = timersMax
	}
}

// 重写 String 方法
func (c *Config) String() string {
	return fmt.Sprintf(
		"\nTiming Wheel Config:"+
			"\n\tCapacity： %d"+
			"\n\tTick: %d"+
			"\n\tTimers: %d\n",
		c.Capacity,
		c.Tick,
		c.Timers)
}
