// timing wheel 时间轮
package timingwheel

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// 定义时间轮状态
type twState = uint64

const (
	// 运行状态
	twRunning twState = iota

	// 停止状态
	twStopped
)

// TimingWheel 定义时间轮
type TimingWheel struct {
	// 时间轮名称,用于识别多层时间轮
	name string

	// 时间轮状态
	state twState

	// 延迟队列
	delayQueue *delayQueue

	// 时间轮单圈容量
	capacity int

	// 时间刻度
	tick int64

	// 一个时间周期的总时长 capacity * tick
	circle int64

	// 当前时间，当前时间毫秒值即优先级
	current int64

	// 桶集合
	buckets []*bucket

	// 下一层时间轮,超过当前时间轮时间周期
	overflow unsafe.Pointer

	// 时间轮调度定时器次数
	count int

	// 互斥锁
	mu sync.Mutex

	// 任务通道
	timersCh chan *Timer

	// 退出通道
	exitCh chan struct{}

	waitGroup sync.WaitGroup
}

// NewTimingWheel 基于配置项新建时间轮
func NewTimingWheel(config *Config) *TimingWheel {
	timingWheel := newTimingWheel(config.Capacity, current(config.Tick), config.Tick, newDelayQueue(config.Capacity))
	timingWheel.state = twStopped
	timingWheel.timersCh = make(chan *Timer, config.Timers)
	return timingWheel
}

// newTimingWheel 基于配置项新建时间轮
func newTimingWheel(cap int, cur int64, tick int64, queue *delayQueue) *TimingWheel {
	return &TimingWheel{
		name:       fmt.Sprintf("TW_%d_%d", cap, tick),
		capacity:   cap,
		delayQueue: queue,
		current:    cur,
		tick:       tick,
		circle:     int64(cap) * tick,
		buckets:    newBuckets(cap),
		overflow:   nil,
		count:      0,
	}
}

// current 根据时间刻度, 取整当前时间
func current(tick int64) int64 {
	milli := time.Now().UnixNano() / 1e6
	if tick == 0 {
		return milli
	}
	return milli - milli%tick
}

// newBuckets 根据时间轮容量新建所有桶
func newBuckets(cap int) []*bucket {
	buckets := make([]*bucket, cap)

	// 初始化所有桶
	for i := range buckets {
		buckets[i] = newBucket()
	}
	return buckets
}

// Start 时间轮开始运行
func (tw *TimingWheel) Start() {
	// 限制重复开始
	if atomic.CompareAndSwapUint64(&tw.state, twStopped, twRunning) {

		// 获取当前时间
		tw.current = current(tw.tick)

		// 时间轮退出通道
		tw.exitCh = make(chan struct{})

		// 获取延迟队列中的桶
		bucketCh := make(chan *bucket)

		tw.waitGroup.Add(1)
		go func() {
			// 启动延迟队列
			tw.delayQueue.run(bucketCh, tw.exitCh)
			// 等待退出
			tw.waitGroup.Done()
		}()

		tw.waitGroup.Add(1)
		go func() {
			for {
				select {
				case <-tw.exitCh:
					goto DONE
				case bucket := <-bucketCh:
					// 获取到桶
					// 根据桶的过期时间进行校准处理
					tw.advanced(bucket.getExpiration())
					// 调度桶中所有的定时器
					bucket.flush(tw.schedule)
				}
			}
		DONE:
			tw.waitGroup.Done()
		}()

		tw.waitGroup.Add(1)
		go func() {
			for {
				select {
				case <-tw.exitCh:
					goto DONE
				case timer := <-tw.timersCh:
					// 运行定时器任务
					go tw.runTask(timer)
				}
			}
		DONE:
			tw.waitGroup.Done()
		}()
	}
}

// advanced 根据桶的过期时间进行校准处理，
func (tw *TimingWheel) advanced(exp int64) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if exp >= tw.current+tw.tick {
		// 根据桶的过期时间校准时间轮当前时间
		tw.current = expireTick(exp, tw.tick)

		// 校准下一层时间轮当前时间
		ow := atomic.LoadPointer(&tw.overflow)
		if ow != nil {
			(*TimingWheel)(ow).advanced(tw.current)
		}
	}
}

// 根据时间间隔, 取整时间
func expireTick(exp, tick int64) int64 {
	if tick == 0 {
		return exp
	}
	return exp - exp%tick
}

// schedule 时间轮调度
func (tw *TimingWheel) schedule(t *Timer) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	// 定时器已经过期
	if timerHasExpired(t.getExpiration(), tw.current+tw.tick) {
		// 计数器
		tw.count++

		// 定时器并发通道
		tw.timersCh <- t
		return
	}

	// 定时器过期时间在时间轮时间周期内
	if timerInCircle(t.getExpiration(), tw.current+tw.circle) {
		// 计算时间轮对应的索引值
		interval := t.getExpiration() / tw.tick
		index := interval % int64(tw.capacity)
		bucket := tw.buckets[index]

		// 将定时器放入对应的桶
		err := bucket.addTimer(t)
		if err != nil {
			return
		}

		// 设置桶的过期时间
		// 将该桶插入到延迟队列中
		if bucket.setExpiration(interval * tw.tick) {
			tw.delayQueue.push(bucket)
		}
		return
	}

	// 定时器超过当前时间轮层的时间周期
	// 新建下一层时间轮,间隔时间为本层时间轮时间周期
	atomic.CompareAndSwapPointer(&tw.overflow, nil, unsafe.Pointer(newTimingWheel(
		tw.capacity, tw.current, tw.circle, tw.delayQueue)))
	ow := atomic.LoadPointer(&tw.overflow)
	// 在下一层时间轮调度该定时器
	(*TimingWheel)(ow).schedule(t)
}

// timerHasExpired 定时器已过期
func timerHasExpired(timerExp, current int64) bool {
	return timerExp < current
}

// 定时器在当前时间轮周期中
func timerInCircle(timerExp, circle int64) bool {
	return timerExp < circle
}

// runTask 运行定时器任务
func (tw *TimingWheel) runTask(t *Timer) {

	// 是否重复执行此定时器
	repeat, err := t.task()

	if err != nil {
		return
	}

	// 重复执行
	if repeat {
		tw.mu.Lock()
		// 设置定时器过期时间
		if t.getDelay() < tw.tick {
			t.setExpiration(tw.current + tw.tick)
		} else {
			t.setExpiration(tw.current + t.getDelay())
		}
		tw.mu.Unlock()
		// 再次调度该定时器
		tw.schedule(t)
	}
}

// Stop 时间轮停止
func (tw *TimingWheel) Stop() {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	// 时间轮转换为停止状态
	if atomic.CompareAndSwapUint64(&tw.state, twRunning, twStopped) {
		// 关闭
		close(tw.exitCh)
		// 等待
		tw.waitGroup.Wait()
		// 清除时间轮
		tw.clear()
		// 清除延迟队列
		tw.delayQueue.queue.Clear()
		// 清除定时器通道
		close(tw.timersCh)
	}

	fmt.Println("count: ", tw.count)
}

// clear 逐层清空时间轮
func (tw *TimingWheel) clear() {
	ow := atomic.LoadPointer(&tw.overflow)
	if ow != nil {
		(*TimingWheel)(ow).clear()
		(*TimingWheel)(ow).delayQueue.queue.Clear()
		for _, bucket := range tw.buckets {
			// 清空桶中的定时器
			bucket.flush(func(t *Timer) {
				t.Stop()
			})
		}
		tw.overflow = nil
	}
}

// AfterFunc 时间轮定时器函数
func (tw *TimingWheel) AfterFunc(id string, delay int64, f TaskFunc) (*Timer, error) {

	if atomic.LoadUint64(&tw.state) == twRunning {
		// 新建定时器
		t := &Timer{id: id, delay: delay, expiration: tw.relativeDelay(delay), task: f}

		// 调度定时器
		go tw.schedule(t)

		return t, nil
	}
	return &Timer{}, errors.New("时间轮未运行")
}

// 时间轮相对当前时间的延迟
func (tw *TimingWheel) relativeDelay(delay int64) int64 {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	return tw.current + delay
}
