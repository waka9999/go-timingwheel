package timingwheel

import (
	"container/heap"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	queue "github.com/waka9999/go-queue"
)

// queueState 队列状态
type queueState = uint64

const (
	// 休眠状态
	dqSleep queueState = iota

	// 就绪状态
	dqReady

	// 挂起状态
	dqPending

	// 调度状态
	dqReschedule
)

// delayQueue 定义延迟队列
// 延迟队列基于优先级队列，支持按优先级进行调度
type delayQueue struct {
	// 优先级队列
	queue *queue.PriorityQueue

	// 队列互斥锁
	mu sync.Mutex

	// 队列状态
	state queueState

	// 就绪通道
	readyCh chan struct{}

	// 调度通道
	reschdCh chan struct{}
}

// newDelayQueue 新建延迟队列
// cap 延迟队列容量
func newDelayQueue(cap int) *delayQueue {
	return &delayQueue{
		queue:    queue.NewPriorityQueue(cap),
		state:    dqSleep,
		readyCh:  make(chan struct{}),
		reschdCh: make(chan struct{}),
	}
}

// insert 延迟队列插入桶
func (dq *delayQueue) push(b *bucket) {
	// 新建队列存储项目, 桶的过期时间作为存储项优先级
	item := queue.NewItem(b, b.expiration)

	dq.mu.Lock()
	heap.Push(dq.queue, item)
	dq.mu.Unlock()

	// 队列中首个存储项目
	if item.Index() == 0 {

		// 首个插入存储项, 队列状态从休眠转换为就绪
		if atomic.CompareAndSwapUint64(&dq.state, dqSleep, dqReady) {
			dq.readyCh <- struct{}{}
			return
		}

		// 非首次插入存储项目，如果队列处于挂起状态，转为调度状态
		if atomic.CompareAndSwapUint64(&dq.state, dqPending, dqReschedule) {
			dq.reschdCh <- struct{}{}
		}
	}
}

// pop 获取队列中的首个桶和优先级差值
// priority 优先级参数，用于和队列中首个桶的优先级进行比较
// 差值 0：队列空，>0 优先级低，-1 弹出
func (dq *delayQueue) pop(priority int64) (*bucket, int64) {
	dq.mu.Lock()
	defer dq.mu.Unlock()

	// 队列为空，返回空桶
	if dq.queue.Len() == 0 {
		return &bucket{}, 0
	}

	// 获取队列中的第一个存储项
	item := dq.queue.First()

	// 如果第一个存储项的优先级（过期时间）大于传入的优先级（过期时间），返回差值
	if item.Priority() > priority {
		return &bucket{}, item.Priority() - priority
	}

	// 否则,弹出第一个桶
	heap.Pop(dq.queue)

	return item.Value().(*bucket), -1
}

// run 延迟队列运行
// bch 通过 channel 获取桶，ech 退出 channel
func (dq *delayQueue) run(bch chan<- *bucket, ech <-chan struct{}) error {
	for {
		// 尝试获取队列中的第一个桶, 传入当前时间毫秒值作为优先级
		bucket, diff := dq.pop(time.Now().UnixNano() / 1e6)

		// 返回首个桶
		if diff == -1 {
			select {
			case <-ech:
				return errors.New("exit -1")
			case bch <- bucket:
				// 等待从 bucket channel 中取走该桶
				continue
			}
		}

		// 返回与队列中第一个桶优先级的差值
		if diff > 0 {
			// 队列转换到挂起状态
			atomic.StoreUint64(&dq.state, dqPending)

			select {
			case <-ech:
				return errors.New("exit >0")
			case <-time.After(time.Duration(diff) * time.Millisecond):
				// 经过差值毫米时间后
				// 队列从挂起状态转换为调度状态,等待重新执行循环
				atomic.StoreUint64(&dq.state, dqReschedule)
				continue
			case <-dq.reschdCh:
				// 有新的存储项插入,队列转换为调度状态,等待重新执行循环
				continue
			}
		}

		// 延迟队列为空
		if diff == 0 {
			// 队列进入休眠状态
			atomic.StoreUint64(&dq.state, dqSleep)

			select {
			case <-ech:
				// 休眠状态退出
				return errors.New("exit 0")
			case <-dq.readyCh:
				// 插入第一个桶,队列转为就绪状态
				continue
			}
		}
	}
}
