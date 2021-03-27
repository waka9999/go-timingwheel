package timingwheel

import (
	"fmt"
	"testing"
	"time"
)

var cfg *Config
var tw *TimingWheel

func TestMain(m *testing.M) {
	cfg = DefaultConfig()
	tw = NewTimingWheel(cfg)
	tw.Start()
	time.Sleep(time.Second)
	m.Run()
}

// func TestAfterFunc(t *testing.T) {
// 	t.Parallel()
// 	tw.AfterFunc("0", 2000, func() (b bool, e error) {
// 		fmt.Println("tast-0", time.Now())
// 		return true, nil
// 	})
// 	select {}
// }

// func TestAfterFuncA(t *testing.T) {
// 	t.Parallel()
// 	for i := 0; ; i++ {
// 		go func(i int) {
// 			tw.AfterFunc(fmt.Sprintf("A-%d", i), 100, func() (b bool, e error) {
// 				fmt.Println(fmt.Sprintf("A-%d", i), time.Now())
// 				return false, nil
// 			})
// 		}(i)
// 	}
// }

// func TestAfterFuncB(t *testing.T) {
// 	t.Parallel()
// 	for i := 0; i < 1000; i++ {
// 		go func(i int) {
// 			tw.AfterFunc(fmt.Sprintf("B-%d", i), 700, func() (b bool, e error) {
// 				fmt.Println(fmt.Sprintf("B-%d", i), time.Now())
// 				return false, nil
// 			})
// 		}(i)
// 	}
// }

func TestAfterFuncC(t *testing.T) {
	t.Parallel()
	for i := 0; i < 100; i++ {
		go func(i int) {
			tw.AfterFunc(fmt.Sprintf("C-%d", i), 40*int64(i), func() (b bool, e error) {
				fmt.Println(fmt.Sprintf("C-%d", i), time.Now())
				return false, nil
			})
		}(i)
	}
}

func TestAfterFuncD(t *testing.T) {
	t.Parallel()
	for i := 0; i < 1000; i++ {
		go func(i int) {
			tw.AfterFunc(fmt.Sprintf("D-%d", i), 5*int64(i), func() (b bool, e error) {
				fmt.Println(fmt.Sprintf("D-%d", i), time.Now())
				return false, nil
			})
		}(i)
	}
}

func TestAfterFuncE(t *testing.T) {
	t.Parallel()
	for i := 0; i < 5; i++ {
		time.Sleep(time.Second)
	}
	tw.Stop()
}
