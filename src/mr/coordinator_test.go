package mr

import (
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 测试两层嵌套的goRoutine，第一层退出后，在程序没结束的情况下，第二层会不会退出
// 结论：不会退出
func TestCoordinator_nestedGoRoutine(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func(index int) {
			defer wg.Done()
			go func() {
				time.Sleep(3 * time.Second)
				fmt.Println(index)
			}()
		}(i)
	}
	wg.Wait()
	fmt.Println("第一层协程全部退出")
	time.Sleep(10 * time.Second) // 等协程的协程跑完
}

// 测试用一个原子变量+cas锁能否保证只do一次。go提供的sync.once里用了lock
func TestCoordinator_CASONCE(t *testing.T) {
	once := int32(0)
	successRoutine := int32(0)
	nRoutine := 1000
	wg := sync.WaitGroup{}
	wg.Add(nRoutine)

	for i := 0; i < nRoutine; i++ {
		go func() {
			defer wg.Done()
			if rand.Int()%4 == 1 {
				time.Sleep(time.Millisecond * 500)
			}
			if atomic.CompareAndSwapInt32(&once, 0, 1) {
				fmt.Printf("success")
				atomic.AddInt32(&successRoutine, 1)
			}
		}()
	}

	wg.Wait()
	reflect.DeepEqual(successRoutine, 1)
}

// 结果：fatal error: all goroutines are asleep - deadlock!
func TestCoordinator_SleepChanGet(t *testing.T) {
	var ch chan int

	go func() {
		time.Sleep(1 * time.Second)
		ch <- 1
	}()

	go func() {
		time.Sleep(2 * time.Second)
		ch <- 2
	}()

	res := <-ch
	print(res)
}
