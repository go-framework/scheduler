package scheduler

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/json-iterator/go"
	"github.com/rcrowley/go-metrics"

	"github.com/go-framework/config/redis"
	"github.com/go-framework/zap"
)

var scheduler Scheduler = nil
var mRedis *miniredis.Miniredis = nil

func initScheduler(ctx context.Context) error {

	if scheduler != nil {
		return nil
	}

	m, err := miniredis.Run()
	if err != nil {
		panic(err)
	}

	mRedis = m

	key := "SchedulerTest"

	redisConfig := redis.DefaultRedisConfig
	redisConfig.Addr = m.Addr()

	zapConfig := zap.GetDebugConfig()
	// zapConfig.Console = false

	queue := NewRedisQueue(key, redisConfig)
	queue.Register(new(testRunner))

	ctx = NewZapConfigContext(ctx, zapConfig)
	zapConfigForRunner := zapConfig.Clone()
	// zapConfigForRunner.Console = false

	store := NewMapStore()

	ctx = NewRunnerZapConfigContext(ctx, zapConfigForRunner)

	scheduler = New(queue, store, 10)

	return scheduler.Init(ctx)
}

func Test_EngineMarshalJSON(t *testing.T) {
	err := initScheduler(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	bytes, err := jsoniter.Marshal(scheduler)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(bytes))
}

func TestEngine_Run_Timeout(t *testing.T) {

	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	err := initScheduler(ctx)
	if err != nil {
		t.Fatal(err)
	}

	err = scheduler.Run()
	if err != context.Canceled {
		t.Log(err)
	} else if err != nil {
		t.Fatal(err)
	}
}

func TestEngine_Run_Canceled(t *testing.T) {

	ctx, canceled := context.WithCancel(context.Background())

	time.AfterFunc(5*time.Second, func() {
		canceled()
	})

	err := initScheduler(ctx)
	if err != nil {
		t.Fatal(err)
	}

	err = scheduler.Run()
	if err != context.Canceled {
		t.Log(err)
	} else if err != nil {
		t.Fatal(err)
	}
}

func TestEngine_Push(t *testing.T) {

	err := initScheduler(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	rand.Seed(time.Now().Unix())

	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("Test-%d", i)

		random := 5 + rand.Int31n(10)

		runner := newTestRunner(name, 60, time.Duration(random)*time.Second)

		err := scheduler.GetQueuer().Push(runner)
		if err != nil {
			t.Fatal(err)
		}

		t.Log(runner)
	}

	t.Log("scheduler size", scheduler.GetQueuer().Length())
}

func TestEngine_Run(t *testing.T) {

	const base = 100
	const max = 300

	random := base + rand.Int63n(max)
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*time.Duration(random))
	defer cancel()

	err := initScheduler(ctx)
	if err != nil {
		t.Fatal(err)
	}

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("Test-%d", i)

		random := 5 + rand.Int31n(10)

		runner := newTestRunner(name, 60, time.Duration(random)*time.Second, withSimulate(true))

		err := scheduler.GetQueuer().Push(runner)
		if err != nil {
			t.Fatal(err)
		}

		t.Log(runner)
	}

	random = base + rand.Int63n(max)
	time.AfterFunc(time.Second*time.Duration(random), func() {
		scheduler.Stop()
	})

	stat := func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				metrics.Log(metrics.DefaultRegistry,
					1*time.Second,
					log.New(os.Stdout, "metrics: ", log.Lmicroseconds))
			}
		}
	}

	go stat()

	err = scheduler.Run()
	if err == context.Canceled || err == context.DeadlineExceeded {
		t.Log(err)
	} else if err == ManualStopErr {
		t.Log(err)
	} else if err != nil {
		t.Fatal(err)
	}
}

func TestMetrics_Gauge(t *testing.T) {
	m := "server.bytes_sent"
	g := metrics.GetOrRegisterGauge(m, nil)
	g.Update(47)
	fmt.Println(g.Value())
	g.Update(4)
	fmt.Println(g.Value())
	g.Update(-4)
	fmt.Println(g.Value())
}

func TestMetrics_Meter(t *testing.T) {
	m := "server.bytes_sent"
	g := metrics.GetOrRegisterMeter(m, nil)
	g.Mark(47)
	fmt.Println(g.Count())
	g.Mark(47)
	fmt.Println(g.Count())
	g.Mark(-47)
	fmt.Println(g.Count())
}

func TestMetrics_Timer(t *testing.T) {
	m := "server.bytes_sent"
	g := metrics.GetOrRegisterTimer(m, nil)
	g.Time(func() {
		time.Sleep(time.Second)
	})
	fmt.Println(g.Count())
	g.Percentile(3)
	fmt.Println(g.Count())
}
