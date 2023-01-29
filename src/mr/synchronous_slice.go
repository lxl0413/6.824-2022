package mr

import (
	"errors"
	"sync"
)

// 其实就是个带同步锁的slice，简化一些代码
type SynchronousSlice[T any] struct {
	mutex    *sync.RWMutex
	elements []T
}

func NewSynchronousSlice[T any]() SynchronousSlice[T] {
	return SynchronousSlice[T]{
		mutex:    &sync.RWMutex{},
		elements: make([]T, 0),
	}
}

func NewSynchronousSliceWithMutex[T any](mutex *sync.RWMutex) SynchronousSlice[T] {
	return SynchronousSlice[T]{
		mutex:    mutex,
		elements: make([]T, 0),
	}
}

// return：append的最后个元素的index
func (l *SynchronousSlice[T]) Append(data ...T) int {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.elements = append(l.elements, data...)
	return len(l.elements) - 1
}

func (l *SynchronousSlice[T]) Set(index int, data T) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if len(l.elements) >= index {
		return errors.New("index out of bound")
	}
	l.elements[index] = data
	return nil
}

func (l *SynchronousSlice[T]) Get(index int) (ret T, err error) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if len(l.elements) >= index {
		return ret, errors.New("index out of bound")
	}
	return l.elements[index], nil
}

func (l *SynchronousSlice[T]) GetElementsSnap() []T {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	res := make([]T, len(l.elements))
	copy(res, l.elements) // copy的长度=min(len(src),len(dst))
	return res
}

func (l *SynchronousSlice[T]) Len() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return len(l.elements)
}
