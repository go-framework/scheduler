package scheduler

import (
	"context"
	"math/rand"
	"testing"
	"time"
)

func TestTestRunner_Run(t *testing.T) {

	rand.Seed(time.Now().UnixNano())
	runner := newTestRunner("test", 5, time.Second*5, withSimulate(true))
	ctx, canceled := context.WithCancel(context.TODO())

	time.AfterFunc(time.Second*5, func() {
		canceled()
	})

	var err error

	defer func() {
		runner.Finalizer(err)

		t.Log(runner.GetState())
		t.Log("after", runner)
	}()

	err = runner.Init(ctx)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("before", runner)

	err = runner.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}

}
