package mr

import (
	"errors"
	"time"
)

type BlockingQueue[T any] struct {
	elements chan T
}

func NewBlockingQueue[T any]() BlockingQueue[T] {
	return BlockingQueue[T]{
		elements: make(chan T, 1000),
	}
}

// 阻塞式写入
func (q *BlockingQueue[T]) offer(data T) error {
	timeout := time.NewTimer(time.Millisecond * 500)

	select {
	case q.elements <- data:
		return nil
	case <-timeout.C:
		return errors.New("offer element time out")
	}
}

// 非阻塞式获取
func (q *BlockingQueue[T]) get() (ret T) {
	select {
	case data := <-q.elements:
		return data
	default:
		return ret
	}
}
