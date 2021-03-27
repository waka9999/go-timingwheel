package timingwheel

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// bucket 时间轮单位"桶"
type bucket struct {
	// 过期时间
	expiration int64

	// 定时器列表
	timerList *list.List

	// 定时器列表互斥锁
	mu sync.Mutex
}

// newBucket 新建桶
func newBucket() *bucket {
	return &bucket{
		expiration: -1,
		timerList:  list.New(),
	}
}

// getExpiration 获得桶的过期时间
func (b *bucket) getExpiration() int64 {
	return atomic.LoadInt64(&b.expiration)
}

// 设置桶的过期时间
// 如果桶的过期时间发生变更返回 true
func (b *bucket) setExpiration(exp int64) bool {
	return atomic.SwapInt64(&b.expiration, exp) != exp
}

// addTimer 添加定时器
func (b *bucket) addTimer(t *Timer) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 将定时器与桶关联
	err := t.inBucket(b)

	if err == nil {
		// 向桶中的定时器列表添加定时器
		e := b.timerList.PushBack(t)
		// 设置列表元素指向 timer 自己
		t.setElement(e)
	}
	return err
}

// flush 清空桶
func (b *bucket) flush(f func(t *Timer)) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for e := b.timerList.Front(); e != nil; {
		next := e.Next()
		t := e.Value.(*Timer)

		// 从桶的定时器列表删除该元定时器
		b.removeElement(t)

		t.outBucket()
		// 调用此定时器任务函数
		f(t)
		e = next
	}
	// 该桶未包含定时器
	b.expiration = -1
}

// removeElement 移除列表元素中的定时器
func (b *bucket) removeElement(t *Timer) {
	// 与定时器所在桶是同一个桶
	if t.bucket != b {
		return
	}

	if t.getElement() != nil || t.bucket.timerList != nil {
		// 移除定时器所对应的列表元素
		b.timerList.Remove(t.getElement())
		t.setElement(nil)
	}
}

// removeTimer 从桶中移除定时器
func (b *bucket) removeTimer(t *Timer) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.removeElement(t)
	t.outBucket()
}
