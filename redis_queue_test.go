package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis"

	"github.com/go-framework/config/redis"
)

func TestRedisQueue_Push(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}

	key := "TestRedisQueue_Push"

	redisConfig := redis.DefaultRedisConfig
	redisConfig.Addr = s.Addr()

	queue := &RedisQueue{
		client:         redisConfig.NewGoClient(),
		privateHashKey: fmt.Sprintf("task@%s#%d", key, time.Now().Unix()),
		hashKey:        "task@" + key,
		setKey:         "queue@" + key,
	}

	var runners []Runner
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("Demo-%d", i)
		runner := newTestRunner(name, 5, time.Second)

		runners = append(runners, runner)
		err = queue.Push(runner)
		if err != nil {
			t.Fatal(err)
		}
	}

	t.Log("hash")
	keys, _ := s.HKeys(queue.hashKey)
	for _, k := range keys {
		t.Log(s.HGet(queue.hashKey, k))
	}

	t.Log("sort set")
	members, _ := s.ZMembers(queue.setKey)
	for _, member := range members {
		score, _ := s.ZScore(key, member)
		t.Log(int64(score), member)
	}

	t.Log("private hash")
	keys, _ = s.HKeys(queue.privateHashKey)
	for _, k := range keys {
		t.Log(s.HGet(queue.privateHashKey, k))
	}

}

func TestRedisQueue_Pop(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}

	key := "TestRedisQueue_Push"

	redisConfig := redis.DefaultRedisConfig
	redisConfig.Addr = s.Addr()

	queue := &RedisQueue{
		client:         redisConfig.NewGoClient(),
		privateHashKey: fmt.Sprintf("task@%s#%d", key, time.Now().Unix()),
		hashKey:        "task@" + key,
		setKey:         "queue@" + key,
	}

	queue.Register(new(testRunner))

	t.Log("push runners")
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("Demo-%d", i)
		runner := newTestRunner(name, 10, time.Second, withTaskOption(WithActionTime(time.Now().Unix()-1000)))
		err = queue.Push(runner)
		if err != nil {
			t.Fatal(err)
		}
		t.Log(runner)
	}

	t.Log("hash")
	keys, _ := s.HKeys(queue.hashKey)
	for _, k := range keys {
		t.Log(s.HGet(queue.hashKey, k))
	}

	t.Log("sort set")
	members, _ := s.ZMembers(queue.setKey)
	for _, member := range members {
		score, _ := s.ZScore(key, member)
		t.Log(int64(score), member)
	}

	t.Log("private hash")
	keys, _ = s.HKeys(queue.privateHashKey)
	for _, k := range keys {
		t.Log(s.HGet(queue.privateHashKey, k))
	}

	runners, err := queue.Pop(context.TODO(), 100, 0)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("pop runners")
	for _, runner := range runners {
		t.Log(runner)
	}

	t.Log("after pop")

	t.Log("hash")
	keys, _ = s.HKeys(queue.hashKey)
	for _, k := range keys {
		t.Log(s.HGet(queue.hashKey, k))
	}

	t.Log("sort set")
	members, _ = s.ZMembers(queue.setKey)
	for _, member := range members {
		score, _ := s.ZScore(key, member)
		t.Log(int64(score), member)
	}

	t.Log("private hash")
	keys, _ = s.HKeys(queue.privateHashKey)
	for _, k := range keys {
		t.Log(s.HGet(queue.privateHashKey, k))
	}
}
func TestRedisQueue_MarshalJSON(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}

	key := "TestRedisQueue_Push"

	hashKey := "task@" + key
	setKey := "queue@" + key

	redisConfig := redis.DefaultRedisConfig
	redisConfig.Addr = s.Addr()

	queue := &RedisQueue{
		hashKey: hashKey,
		setKey:  setKey,
		client:  redisConfig.NewGoClient(),
	}

	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("Demo-%d", i)
		runner := newTestRunner(name, 5, time.Second)

		err := runner.Init(context.TODO())
		if err != nil {
			t.Fatal(err)
		}

		err = queue.Push(runner)
		if err != nil {
			t.Fatal(err)
		}
	}

	data, err := queue.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	t.Log(string(data))
}
