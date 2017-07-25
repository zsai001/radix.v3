package main

import (
	"flag"
	"fmt"
	"github.com/zsai001/radix.v3/pool"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func UnixTimeNow() float64 {
	now := time.Now().UnixNano()
	return float64(now) / 1000.0 / 1000.0 / 1000.0
}

var begin float64 = 0.0
var count int32 = 0
var countLock sync.Mutex = sync.Mutex{}

func AddCount(check float64) bool {
	now := UnixTimeNow()
	atomic.AddInt32(&count, 1)
	if begin < now-check {
		fmt.Println("ops", count, "in", check, "seconds")
		begin = now
		count = 0
		return true
	}
	return false
}

func main() {
	fmt.Println("main function begin")
	cmd := flag.String("cmd", "PING", "test redis command. ex.PING")
	addr := flag.String("h", "127.0.0.1:6379", "addr to test")
	out := flag.Float64("f", 1, "time to print")
	flag.Parse()

	//client, err := redis.DialTimeout("tcp", "127.0.0.1:6379", 10*time.Second)
	client, err := pool.New("tcp", *addr, 1)

	if err != nil {
		fmt.Println(err)
		return
	}

	cmdList := strings.Split(*cmd, " ")
	redisCmd := cmdList[0]
	redisArgs := make([]interface{}, len(cmdList)-1)
	for k, v := range cmdList[1:] {
		redisArgs[k] = v
	}

	bench := func() {
		for true {
			info := client.FCmd(redisCmd, redisArgs...)
			//time.Sleep(time.Nanosecond* time.Duration(1000*1000*100/limit_ops))
			if AddCount(*out) {
				fmt.Printf("last cmd resp %v", info.GetResp())
			}
		}
	}

	bench()
}
