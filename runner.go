package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"time"
	"unsafe"

	"github.com/json-iterator/go"
)

//
// id runner.
//
type idRunner struct {
	// id
	id string
}

// Get identifier of runner.
func (r *idRunner) GetId() string {
	return r.id
}

// Get name of runner.
func (r *idRunner) GetName() string {
	return r.id
}

// Get type of runner.
func (r *idRunner) GetType() string {
	return "IdRunner"
}

// Get Runner action Unix time.
func (r *idRunner) GetActionTime() int64 {
	return 0
}

// Get Runner State.
func (r *idRunner) GetState() State {
	return Created
}

// Initialize runner with context, An error occurred will not be call Run function.
func (r *idRunner) Init(context.Context) error {
	return nil
}

// Run with context, Return an error if some wrong.
func (r *idRunner) Run(context.Context) error {
	return nil
}

// Finalizer with error.
func (r *idRunner) Finalizer(error) {}

// Stop runner.
func (r *idRunner) Stop() error {
	return nil
}

// Cancel runner.
func (r *idRunner) Cancel() {
}

// Check Runner is effective.
func (r idRunner) Check() error {
	return nil
}

// Runner Reusable, when true Scheduler will put into Queuer.
func (r idRunner) Reusable() bool {
	return true
}

// Get template of Runner.
func (r *idRunner) GetTemplate(context.Context) []*Template {
	return nil
}

// New Runner.
func (r *idRunner) New() Runner {
	return new(idRunner)
}

// New id runner.
func NewIdRunner(id string) Runner {
	return &idRunner{
		id: id,
	}
}

//
// test runner.
//
type testRunner struct {
	Task
	Duration time.Duration `json:"duration" yaml:"duration"`
	Simulate bool          `json:"simulate" yaml:"duration"`

	duration time.Duration
	exit     chan error
}

// An Option configures a test runner.
type testRunnerOption interface {
	apply(*testRunner)
}

// testRunnerOptionFunc wraps a func so it satisfies the Option interface.
type testRunnerOptionFunc func(*testRunner)

func (f testRunnerOptionFunc) apply(t *testRunner) {
	f(t)
}

// with simulate.
func withSimulate(simulate bool) testRunnerOption {
	return testRunnerOptionFunc(func(t *testRunner) {
		t.Simulate = simulate
	})
}

// with task option.
func withTaskOption(opt TaskOption) testRunnerOption {
	return testRunnerOptionFunc(func(t *testRunner) {
		opt.Apply(&t.Task)
	})
}

// simulate status.
func (r *testRunner) simulateStatus(e error) (err error) {
	states := []State{Succeed, Failed, Retrying, Stopped, Canceled}
	random := rand.Intn(len(states))
	state := states[random]

	r.logger.Sugar().Debug("simulate status ", state)

	switch state {
	case Succeed:
		err = nil
	case Failed:
		err = fmt.Errorf("simulate runner failed")
	case Retrying:
		err = fmt.Errorf("simulate runner retry")
		r.Task.Retrying(time.Second*5, err)
	case Stopped:
		return e
	case Canceled:
		err = fmt.Errorf("simulate runner canceled")
	}

	r.State = state

	return
}

// Get identifier of Runner.
func (r *testRunner) GetId() string {
	return r.Tid
}

// Get type of Runner.
func (r *testRunner) GetType() string {
	return "test"
}

// Initialize Runner with context, An error occurred will not be call Run function.
func (r *testRunner) Init(ctx context.Context) error {
	err := r.Task.Init(ctx)
	r.GetLogger().Sugar().Debug(r.Name, " init...")

	return err
}

// Run with context, Return an error if some wrong.
func (r *testRunner) Run(ctx context.Context) (err error) {
	r.GetLogger().Sugar().Debug(r.Name, " running...")

	// enable simulate.
	if r.Simulate {
		defer func() {
			err = r.simulateStatus(err)
		}()
	}

	defer func() {
		r.GetLogger().Sugar().Debug(r.Name, " stop by ", err)
	}()

	r.Task.Start()

	// run duration.
	time.AfterFunc(r.duration, func() {
		r.Stop()
	})

	var queuer Queuer
	if v, ok := QueuerFromContext(ctx); ok {
		queuer = v
		queuer.Length()
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err = <-r.exit:
			return
		default:
			time.Sleep(time.Second)
		}
	}
}

// Finalizer with error.
func (r *testRunner) Finalizer(err error) {
	defer r.Task.Finalizer(err)
	r.GetLogger().Sugar().Debug(r.Name, " finalizer error: ", err)
	// Finished or Retrying.
	r.Task.Finished(err)
}

// Stop Runner.
func (r *testRunner) Stop() error {
	if err := r.Task.Stop(); err != nil {
		return err
	}

	r.GetLogger().Sugar().Debug("manual stop ", r.Name)

	r.exit <- ManualStopErr

	return ManualStopErr
}

// Cancel Runner.
func (r *testRunner) Cancel() {
	r.GetLogger().Sugar().Debug("manual cancel ", r.Name)

	r.Task.Cancel()
	if r.State == Running {
		r.exit <- ManualCancelErr
	}
}

// Get template of Runner.
func (r testRunner) GetTemplate(ctx context.Context) []*Template {

	templates := r.Task.GetTemplate(ctx)

	templates = append(templates,
		&Template{
			Name:        "Duration",
			Field:       "duration",
			Type:        "time.Duration",
			Must:        true,
			Description: "runner duration.",
			Example:     "5m",
		},
		&Template{
			Name:        "Simulate",
			Field:       "simulate",
			Type:        "boolean",
			Must:        false,
			Description: "simulate enable.",
			Example:     "false",
		})

	templates = r.Task.GetTemplate(ctx)

	return templates
}

// New Runner.
func (r testRunner) New() Runner {

	runner := new(testRunner)
	runner.duration = r.duration
	runner.exit = make(chan error)

	return runner
}

// Check Runner is effective, An error occurred will not be call Run function.
func (r testRunner) Check() error {
	return r.Task.Check()
}

// Implement Stringer.
func (r testRunner) String() string {
	if data, err := jsoniter.Marshal(r); err == nil {
		return *(*string)(unsafe.Pointer(&data))
	}
	return fmt.Sprintf("name: %s period: %d state: %v action: %d elapsed: %d", r.Name, r.Period, r.State, r.ActionAt, r.Elapsed)
}

// new test runner.
func newTestRunner(name string, period int64, duration time.Duration, opts ...testRunnerOption) Runner {
	r := &testRunner{
		exit: make(chan error),
		Task: Task{
			Name:   name,
			Period: period,
		},
		duration: duration,
		Simulate: false,
		Duration: duration,
	}

	// range task options.
	for _, opt := range opts {
		opt.apply(r)
	}

	//  update task property.
	r.SetTid()
	r.Created()

	return r
}
