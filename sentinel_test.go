package sentinel

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
)

func TestSentinel(t *testing.T) {
	st := NewSentinel(
		[]string{"127.0.0.1:26379"},
		"mymaster",
	)
	defer st.Close()

	err := st.Discover()
	if err != nil {
		t.Log(err)
		t.FailNow()
	}

	addrs, err := st.SentinelAddrs()
	t.Log(addrs, err)
	master, err := st.MasterAddr()
	t.Log(master, err)
	slaves, err := st.SlaveAddrs()
	t.Log(slaves, err)

	wg := &sync.WaitGroup{}
	ms, err := st.MasterSwitch()
	w, err := ms.Watch()
	_ = w
	go func() {
		defer wg.Done()
		wg.Add(1)
		for addr := range w {
			t.Log(addr)
		}
		fmt.Println("watch exit")
	}()
	time.Sleep(20 * time.Second)
	fmt.Println("close")
	ms.Close()
	wg.Wait()
}

func TestSentinelPool(t *testing.T) {
	sp := NewSentinelPool([]string{"127.0.0.1:26379"}, "mymaster")
	for i := 0; i < 30; i++ {
		conn := sp.Get()
		if conn == nil {
			fmt.Println("get conn fail, ", i)
			continue
		}
		s, err := redis.String(conn.Do("INFO"))
		if err != nil {
			fmt.Println("do command error:", err)
		} else {
			_ = s
			fmt.Println(i, sp.curAddr)
		}
		time.Sleep(1 * time.Second)
	}
	sp.Close()
}
