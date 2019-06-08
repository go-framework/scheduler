package scheduler

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/json-iterator/go"
	"github.com/rcrowley/go-metrics"
	"go.uber.org/zap"

	zapConfig "github.com/go-framework/zap"

	"github.com/go-framework/errors"
)

// Defined default value.
const (
	DefaultConcurrent        = 20
	DefaultMaxConcurrent     = 1000
	DefaultErrorChanSize     = 32
	DefaultDoneChanSize      = 32
	ScheduleSleep            = time.Second * 1
	EmptyScheduleSleep       = ScheduleSleep * 3
	DefaultPrintStatInterval = time.Second * 10
)

// Schedule implement Scheduler interface, use chan control goroutine count.
// all goroutine put in the same context.
type Schedule struct {
	//
	// private property.
	//
	// once do.
	once sync.Once
	// use chan control goroutine count.
	waiting chan struct{}
	// context.
	context    context.Context
	cancelFunc context.CancelFunc
	// error chan.
	error chan error
	// runner done chan.
	done chan Runner
	// logger.
	logger *zap.Logger
	// queuer.
	queue Queuer
	// runner realtime store.
	store RealtimeStore
	// engine state.
	status State
	// queuer pop offset.
	offset uint64

	// go metrics format
	builder *strings.Builder
	fields  []zap.Field

	//
	// stat metrics.
	//
	*Metrics

	//
	// Schedule config.
	//
	// Stat enabled, default is false.
	EnableStat bool `json:"enable_stat"`
	// Print stat enabled, default is false.
	EnablePrintStat bool `json:"enable_print_stat"`
	// Print stat interval, default is 10s.
	PrintStatInterval time.Duration `json:"print_stat_interval"`
	// Logger config.
	ZapConfig *zapConfig.Config `json:"logger"`
	// Scheduler concurrent.
	Concurrent uint64 `json:"concurrent"`
	// Max of run permit concurrent.
	MaxConcurrent uint64 `json:"max_concurrent"`
}

// scheduler runner, store runner into RealtimeStore, runner's id is the key,
// when runner is finished, remove it, and call finalizer.
func (this *Schedule) scheduler(runner Runner) {

	var err error
	var push = true

	// send runner done.
	defer func() {
		if this.done != nil {
			this.done <- runner
		}
	}()

	// get recover.
	defer func() {
		if r := recover(); r != nil {
			switch v := r.(type) {
			case error:
				err = v
			default:
				err = fmt.Errorf("%v", v)
			}
		}
		// finalizer runner.
		runner.Finalizer(err)
		// get runner status.
		state := runner.GetState()
		switch state {
		case Succeed:
			this.Successes.Mark(1)
		case Failed:
			this.Failures.Mark(1)
		case Retrying:
			this.Retries.Mark(1)
		case Stopped:
			this.Failures.Mark(1)
		case Canceled:
			push = false
			this.Failures.Mark(1)
		default:
			push = false
			this.Failures.Mark(1)
		}

		// push Runner into Queuer.
		if push {
			// runner reusable?
			if runner.Reusable() {
				if err := this.queue.Push(runner); err != nil {
					this.Errors.Mark(1)
					if this.error != nil {
						this.error <- err
					}
				}
				this.logger.Debug("push reusable runner", zap.String("id", runner.GetId()), zap.String("name", runner.GetName()), zap.String("state", runner.GetState().String()), zap.Int64("actionName", runner.GetActionTime()))
			} else {
				this.logger.Debug("give up not reusable runner", zap.String("id", runner.GetId()), zap.String("name", runner.GetName()), zap.String("state", runner.GetState().String()), zap.Bool("reusable", runner.Reusable()))
			}
		} else {
			this.logger.Debug("give up runner", zap.String("id", runner.GetId()), zap.String("name", runner.GetName()), zap.String("state", runner.GetState().String()), zap.Error(err))
		}
		// remove runner from RealtimeStore.
		if err := this.store.Delete(runner); err != nil {
			this.Errors.Mark(1)
			if this.error != nil {
				this.error <- err
			}
		}

		// stat runs.
		this.Runs.Mark(1)

		// receive value from chan, then can run next goroutine.
		<-this.waiting
	}()

	// store runner into RealtimeStore.
	if err := this.store.Put(runner); err != nil {
		this.Errors.Mark(1)
		if this.error != nil {
			this.error <- err
		}
	}

	// stat total and runs.
	this.Total.Mark(1)
	this.Runs.Mark(1)

	// init runner.
	err = runner.Init(this.context)
	if err != nil {
		return
	}

	// check runner.
	err = runner.Check()
	if err != nil {
		push = false
		return
	}

	// run runner.
	err = runner.Run(this.context)

	return
}

// Start run the Schedule.
func (this *Schedule) run() (err error) {
	// get recover.
	defer func() {
		if r := recover(); r != nil {
			switch v := r.(type) {
			case error:
				err = v
			default:
				err = fmt.Errorf("%v", v)
			}
		}
	}()

	// atomic load concurrent.
	count := atomic.LoadUint64(&this.Concurrent)
	if count > this.MaxConcurrent {
		count = this.MaxConcurrent
	}

	// get store, when got context's error then return the function.
	runners, err := this.queue.Pop(this.context, count, this.offset)
	if err == context.Canceled || err == context.DeadlineExceeded {
		return nil
	} else if err != nil {
		this.logger.Error("pop runners error", zap.Error(err))
		return err
	}

	this.logger.Debug("pop runners", zap.Int("count", len(runners)), zap.Int64("length", this.GetQueuer().Length()))

	if len(runners) == 0 {
		this.offset = 0
		time.Sleep(EmptyScheduleSleep)
		return
	} else {
		this.offset += count
	}

	// loop runners.
	for _, runner := range runners {
		this.logger.Debug("waiting ...")
		// send waiting chan, when it's full will be block.
		this.waiting <- struct{}{}
		this.logger.Debug("got signal", zap.String("id", runner.GetId()), zap.String("name", runner.GetName()))
		// go scheduler runner.
		go this.scheduler(runner)
	}

	time.Sleep(ScheduleSleep)

	return
}

// schedule stat metrics.
func (this *Schedule) stat() {
	defer this.logger.Debug("metrics exit...")

	// use ticker control stat metrics.
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	maxConcurrentGauge := metrics.NewGauge()
	if err := metrics.Register("schedule.max_concurrent", maxConcurrentGauge); err != nil {
	}

	concurrentGauge := metrics.NewGauge()
	if err := metrics.Register("schedule.concurrent", concurrentGauge); err != nil {
	}

	queueLengthGauge := metrics.NewGauge()
	if err := metrics.Register("schedule.queue.length", queueLengthGauge); err != nil {
	}

	queueWaitingGauge := metrics.NewGauge()
	if err := metrics.Register("schedule.queue.waiting", queueWaitingGauge); err != nil {
	}

	// loop run.
	for {
		select {
		case <-this.context.Done():
			this.logger.Debug("metrics context exit...")
			return
		case <-ticker.C:
			maxConcurrentGauge.Update(int64(this.getMaxConcurrent()))
			concurrentGauge.Update(int64(this.getConcurrent()))
			queueLengthGauge.Update(this.queue.Length())
			queueWaitingGauge.Update(this.queue.Waiting())
		}
	}
}

// Just for implement go-metrics Logger interface.
func (this *Schedule) Printf(format string, v ...interface{}) {

	if this.builder != nil && !strings.HasPrefix(format, " ") {
		this.logger.Info(this.builder.String(), this.fields...)

		this.builder = nil
		this.fields = nil
	}

	if this.builder == nil {
		this.builder = &strings.Builder{}
	}

	if strings.HasPrefix(format, " ") {
		splits := strings.Split(strings.Replace(format, " ", "", -1), ":")
		this.fields = append(this.fields, zap.Any(splits[0], v[0]))
	} else {
		this.fields = append(this.fields, zap.String("role", "metrics"), zap.String("type", strings.Split(format, " ")[0]))
		this.builder.WriteString(fmt.Sprintf(strings.TrimSpace(format), v...))
	}
}

// schedule print metrics.
func (this *Schedule) printMetrics(freq time.Duration) {
	ticker := time.NewTicker(freq)
	defer ticker.Stop()
	for {
		select {
		case <-this.context.Done():
			this.logger.Debug("metrics context exit...")
			return
		case <-ticker.C:
			metrics.Log(metrics.DefaultRegistry, freq, this)
		}
	}
}

// get MaxConcurrent.
func (this *Schedule) getMaxConcurrent() uint64 {
	return atomic.LoadUint64(&this.MaxConcurrent)
}

// get Concurrent.
func (this *Schedule) getConcurrent() uint64 {
	return atomic.LoadUint64(&this.Concurrent)
}

// Get Context.
func (this *Schedule) GetContext() context.Context {
	return this.context
}

// Get Queuer.
func (this *Schedule) GetQueuer() Queuer {
	return this.queue
}

// Get Logger.
func (this *Schedule) GetLogger() *zap.Logger {
	return this.logger
}

// Initialize Schedule with context.
func (this *Schedule) Init(ctx context.Context) error {
	this.once.Do(func() {
		// set context.
		this.context, this.cancelFunc = context.WithCancel(ctx)
		// get zap config from context.
		if config, ok := ZapConfigFromContext(ctx); ok {
			this.ZapConfig = config
		}

		// if zap config is nil then new as zap debug config.
		if this.ZapConfig == nil {
			this.ZapConfig = zapConfig.GetDebugConfig()
		}

		// get zap option from context.
		if opts, ok := ZapOptionFromContext(ctx); ok {
			this.logger = this.ZapConfig.NewZapLogger(opts...)
		}

		// if logger is nil then new it.
		if this.logger == nil {
			this.logger = this.ZapConfig.NewZapLogger()
		}

		// get zap name from context.
		if name, ok := ZapNameFromContext(ctx); ok {
			this.logger = this.logger.Named(name)
		}

		// with scheduler mark.
		this.logger = this.logger.With(zap.String("mark", "scheduler"))

		// set Queuer into context.
		this.context = NewQueuerContext(this.context, this.queue)

		this.status = Init
	})

	this.logger.Debug("init ...")

	return nil
}

// Start run the Schedule.
func (this *Schedule) Run() (err error) {

	if this.context == nil {
		return errors.New("engine need init first")
	}

	if this.status == Running {
		return errors.New("engine is running")
	}

	defer func() {
		this.status = Stopped
		this.logger.Debug("exit by", zap.Error(err))
	}()

	// start running.
	this.status = Running
	this.logger.Debug("running ...")

	// loop run.
	for {
		select {
		case <-this.context.Done():
			this.logger.Debug("get context signal")
			close(this.waiting)
			return this.context.Err()
		default:
			err = this.run()
			if err != nil {
				this.logger.Error("run error", zap.Error(err))
				this.Errors.Mark(1)
				if this.error != nil {
					this.error <- err
				}
			}
		}
	}
}

// Stop run the Schedule.
func (this *Schedule) Stop() error {
	if this.status != Running {
		return errors.New("task is not running")
	}

	defer this.cancelFunc()

	if this.error != nil {
		this.error <- ManualStopErr
	}

	return ManualStopErr
}

// Stop a runner by id.
func (this *Schedule) StopRunner(id string) (err error) {
	this.logger.Debug("stop runner", zap.Any("id", id))

	var runner Runner
	// load by id and stopped.
	runner, err = this.store.Get(id)
	if err != nil {
		this.Errors.Mark(1)
		if this.error != nil {
			this.error <- err
		}
	}

	return runner.Stop()
}

// Remove a runner by id.
func (this *Schedule) RemoveRunner(id string) (err error) {
	this.logger.Debug("remove runner", zap.Any("id", id))

	var runner Runner
	// load by id and remove.
	runner, err = this.store.Get(id)
	if err != nil {
		this.Errors.Mark(1)
		if this.error != nil {
			this.error <- err
		}
		return
	}

	runner.Cancel()

	if err = this.store.Delete(runner); err != nil {
		this.Errors.Mark(1)
		if this.error != nil {
			this.error <- err
		}
	}

	if err = this.queue.Remove(runner); err != nil {
		this.Errors.Mark(1)
		if this.error != nil {
			this.error <- err
		}
	}

	return
}

// Get the engine error.
func (this *Schedule) Error() <-chan error {
	if this.error == nil {
		this.error = make(chan error, DefaultErrorChanSize)
	}
	return this.error
}

// Get runner done.
func (this *Schedule) Done() <-chan Runner {
	if this.done == nil {
		this.done = make(chan Runner, DefaultDoneChanSize)
	}
	return this.done
}

// Implement Marshaler.
func (this Schedule) MarshalJSON() (data []byte, err error) {
	// bytes buffer.
	buffer := bytes.Buffer{}

	buffer.WriteByte('{')

	// Queue
	// data, err = jsoniter.Marshal(this.queue)
	// if err != nil {
	// 	return nil, err
	// }
	// buffer.WriteString(`"queue":`)
	// buffer.Write(data)

	// store
	data, err = jsoniter.Marshal(this.store)
	if err != nil {
		return nil, err
	}
	buffer.WriteString(`"store":`)
	buffer.Write(data)

	// status
	data, err = jsoniter.Marshal(this.status)
	if err != nil {
		return nil, err
	}
	buffer.WriteString(`,"status":`)
	buffer.Write(data)

	// errors
	var array []byte
	array = strconv.AppendInt(array, this.Errors.Count(), 10)
	buffer.Write(array)

	// total
	buffer.WriteString(`,"total":`)
	array = nil
	array = strconv.AppendInt(array, this.Total.Count(), 10)
	buffer.Write(array)

	// runs
	buffer.WriteString(`,"runs":`)
	array = nil
	array = strconv.AppendInt(array, this.Runs.Count(), 10)
	buffer.Write(array)

	// failures
	buffer.WriteString(`,"failures":`)
	array = nil
	array = strconv.AppendInt(array, this.Failures.Count(), 10)
	buffer.Write(array)

	// retries
	buffer.WriteString(`,"failures":`)
	array = nil
	array = strconv.AppendInt(array, this.Retries.Count(), 10)
	buffer.Write(array)

	// ZapConfig -> logger
	data, err = jsoniter.Marshal(this.ZapConfig)
	if err != nil {
		return nil, err
	}
	buffer.WriteString(`,"logger":`)
	buffer.Write(data)

	// Concurrent
	concurrent := atomic.LoadUint64(&this.Concurrent)
	buffer.WriteString(`,"concurrent":`)
	array = nil
	array = strconv.AppendUint(array, concurrent, 10)
	buffer.Write(array)

	// MaxConcurrent
	array = nil
	buffer.WriteString(`,"max_concurrent":`)
	array = strconv.AppendUint(array, uint64(this.MaxConcurrent), 10)
	buffer.Write(array)

	buffer.WriteByte('}')

	return buffer.Bytes(), nil
}

// New scheduler with params.
func New(queue Queuer, store RealtimeStore, opts ...ScheduleOption) Scheduler {

	// new engine.
	e := &Schedule{
		queue:             queue,
		store:             store,
		status:            Created,
		EnableStat:        false,
		EnablePrintStat:   false,
		PrintStatInterval: DefaultPrintStatInterval,
		Concurrent:        DefaultConcurrent,
		MaxConcurrent:     DefaultMaxConcurrent,
		Metrics:           NewMetrics(),
	}

	// must set queue.
	if queue == nil {
		panic("queue should not be nil")
	}

	// range options.
	for _, opt := range opts {
		opt.Apply(e)
	}

	// use default.
	if e.MaxConcurrent == 0 {
		e.MaxConcurrent = DefaultMaxConcurrent
	}

	e.waiting = make(chan struct{}, e.MaxConcurrent)

	// go print metrics.
	if e.EnablePrintStat {
		e.EnableStat = true
		go e.printMetrics(e.PrintStatInterval)
	}

	// go stat metrics.
	if e.EnableStat {
		go e.stat()
	}

	return e
}
