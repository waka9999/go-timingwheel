package timingwheel

import (
	"container/list"
	"errors"
	"sync/atomic"
)

// 定义定时器状态
type timerState = uint64

const (
	timerIdle    timerState = iota // 空闲状态，不在桶中
	timerWait                      // 等待状态
	timerStopped                   // 停止状态
)

// TaskFunc 定义任务函数
type TaskFunc func() (bool, error)

// Timer 定义定时器
type Timer struct {
	// 定时器 ID
	id string

	// 所在桶
	bucket *bucket

	// 定时器要执行的任务函数
	task TaskFunc

	// 定时器过期时间
	expiration int64

	// 定时器延迟时间
	delay int64

	// 定时器状态
	state timerState

	// 列表元素（定时器自己）
	element *list.Element
}

// getExpiration 获取定时器过期时间
func (t *Timer) getExpiration() int64 {
	return t.expiration
}

// setExpiration 设置定时器过期时间
func (t *Timer) setExpiration(exp int64) {
	t.expiration = exp
}

// getDelay 获取定时器延迟时间
func (t *Timer) getDelay() int64 {
	return t.delay
}

// inBucket 设置定时器所在的桶
func (t *Timer) inBucket(b *bucket) error {
	if b != nil {

		// 空闲状态的定时器才能设置所在桶，并转换为等待状态
		if atomic.CompareAndSwapUint64(&t.state, timerIdle, timerWait) {
			t.bucket = b
			return nil
		}
		return errors.New("不能将定时器放入桶中")
	}
	return errors.New("桶为 nil")
}

// outBucket 定时器移除所在桶
func (t *Timer) outBucket() {
	// 如果定时器状态是等待转换为空闲状态
	if atomic.CompareAndSwapUint64(&t.state, timerWait, timerIdle) {
		t.bucket = nil
	}
}

// setElement 设置定时器元素
func (t *Timer) setElement(e *list.Element) {
	t.element = e
}

// getElement 获取定时器元素
func (t *Timer) getElement() *list.Element {
	return t.element
}

// Stop 停止定时器
func (t *Timer) Stop() {
	if atomic.LoadUint64(&t.state) == timerStopped {
		return
	}

	for {
		// 将空闲状态的定时器转换为停止状态
		if atomic.CompareAndSwapUint64(&t.state, timerIdle, timerStopped) {
			break
		}

		// 在桶中的定时器为等待状态,需要先移除桶中的定时器
		if t.bucket != nil {
			t.bucket.removeTimer(t)
		}
	}
}
