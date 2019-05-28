package scheduler

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"math/rand"
	"time"

	"github.com/go-framework/zap"
)

// Defined default value.
const (
	DefaultPeriod    = 60 * 5
	ActionBaseTime   = 3
	ActionRandomTime = 3
)

// Task defined.
type Task struct {
	//
	// task private property.
	//
	// task logger.
	logger *zap.Logger
	//
	//
	// Task property.
	//
	// Task id is hash(Name).
	Tid string `json:"tid"`
	// Task status.
	Status State `json:"status"`
	// Task create time.
	CreatedAt int64 `json:"created_at"`
	// Task update time.
	UpdatedAt int64 `json:"updated_at"`
	// Task action time.
	ActionAt int64 `json:"action_at"`
	// Task run count.
	Runs uint64 `json:"runs"`
	// Task run succeed count.
	Successes uint64 `json:"successes"`
	// Task retry times.
	Retries uint8 `json:"retries"`
	// Task run elapsed second.
	Elapsed int64 `json:"elapsed"`
	// Task run error.
	Error string `json:"error"`
	//
	// Task config.
	//
	// Task unique name.
	Name string `json:"name"`
	// Pid is Task parent'Tid.
	Pid string `json:"pid"`
	// Task type.
	Type string `json:"type"`
	// Task run period second.
	Period int64 `json:"period"`
	// Task expire time: 0 - never expired.
	ExpiredAt int64 `json:"expired_at"`
	// Task run max retry count: 0 - unlimited.
	MaxRetries uint8 `json:"max_retries"`
	// Task max permit run count: 0 - unlimited.
	MaxRuns uint64 `json:"max_runs"`
	// Task logger config.
	ZapConfig *zap.Config `json:"logger"`
}

// stat Task run elapsed and times.
func (t *Task) stat() {
	t.Elapsed = time.Now().Unix() - t.Elapsed
	t.Runs++
}

// Set Task tid.
func (t *Task) SetTid() {
	h := sha1.New()
	io.WriteString(h, t.Name)
	bs := h.Sum(nil)

	t.Tid = hex.EncodeToString(bs)
}

// New Task logger.
func (t *Task) NewLogger(ctx context.Context) *zap.Logger {

	// for use unmarshal config.
	if t.ZapConfig == nil {
		// get zap config from context for runner.
		if config, ok := RunnerZapConfigFromContext(ctx); ok {
			t.ZapConfig = config
		}
	}

	// for use unmarshal config.
	if t.ZapConfig == nil {
		// get zap config from context.
		if config, ok := ZapConfigFromContext(ctx); ok {
			t.ZapConfig = config
		}
	}

	// if Logger is nil then new as zap debug config.
	if t.ZapConfig == nil {
		t.ZapConfig = zap.GetDebugConfig()
	}

	// get zap option from context for runner.
	if opts, ok := RunnerZapOptionFromContext(ctx); ok {
		t.logger = t.ZapConfig.NewZapLogger(opts...)
	}

	// get zap option from context.
	if t.logger == nil {
		if opts, ok := ZapOptionFromContext(ctx); ok {
			t.logger = t.ZapConfig.NewZapLogger(opts...)
		}
	}

	// if logger is nil then new it.
	if t.logger == nil {
		t.logger = t.ZapConfig.NewZapLogger()
	}

	return t.logger
}

// Get Task logger.
func (t *Task) GetLogger() *zap.Logger {
	if t.logger == nil {
		t.NewLogger(context.TODO())
	}

	return t.logger
}

// Clone Task config except task name.
func (t Task) Clone() Task {
	return Task{
		Period:     t.Period,
		ExpiredAt:  t.ExpiredAt,
		MaxRetries: t.MaxRetries,
		MaxRuns:    t.MaxRuns,
		ZapConfig:  t.ZapConfig,
	}
}

// Judge Task can action.
func (t *Task) CanAction() bool {
	now := time.Now().Unix()
	if now >= t.ActionAt && (t.MaxRuns == 0 || t.Runs < t.MaxRuns) {
		switch t.Status {
		case Created, Succeed, Failed:
			return true
		case Queued:
			return false
		case Running, Retrying:
			if now > (t.ActionAt + 3*int64(t.Period)) {
				return true
			}
		}
	}

	return false
}

// Judge Task can retrying.
func (t *Task) CanRetrying() bool {
	if t.Retries < t.MaxRetries || t.MaxRetries == 0 {
		return true
	}
	return false
}

// Update action time as time now plus Task period.
func (t *Task) UpdateActionTime() {
	t.ActionAt = time.Now().Unix() + t.Period
}

// Task Created function. set Task created at as time now, status as Created,
// if not set action at then equal created at.
func (t *Task) Created() {
	t.CreatedAt = time.Now().Unix()
	t.Status = Created
	if t.ActionAt == 0 {
		random := ActionBaseTime + rand.Int63n(ActionRandomTime)
		t.ActionAt = t.CreatedAt + random
	}
}

// Task is in queued.
func (t *Task) InQueued() {
	t.Status = Queued
}

// Start Task.
func (t *Task) Start() {
	t.Elapsed = time.Now().Unix()
	t.Status = Running
}

// Task run succeed.
func (t *Task) Succeed() {
	t.Status = Succeed
	t.Error = ""
	t.Successes++
}

// Task run failed with error.
func (t *Task) Failed(err error) {
	// comment it then can Stopped, Retrying, Canceled state.
	// t.Status = Failed
	t.Error = err.Error()
	t.Retries = 0
}

// Task retrying after delay times.
func (t *Task) Retrying(delay time.Duration, err error) {
	t.ActionAt = time.Now().Add(delay).Unix()
	t.Status = Retrying
	t.Error = err.Error()
	t.Retries++
}

// Task run finished with error.
func (t *Task) Finished(err error) {
	if err != nil {
		t.Failed(err)
	} else {
		t.Succeed()
	}
}

// Get type of runner.
func (t *Task) GetType() string {
	return t.Type
}

// Get template of Runner.
func (t Task) GetTemplate(ctx context.Context) []*Template {
	templates := []*Template{
		&Template{
			Name:        "Name",
			Field:       "name",
			Type:        "string",
			Must:        true,
			Description: "unique task name.",
			Example:     "demo-test",
		},
		&Template{
			Name:        "Type",
			Field:       "type",
			Type:        "string",
			Must:        true,
			Description: "name's type.",
			Example:     "demo",
		},
		&Template{
			Name:        "Pid",
			Field:       "pid",
			Type:        "string",
			Must:        false,
			Description: "task parent's id.",
			Example:     "demo",
		},
		&Template{
			Name:        "Period",
			Field:       "period",
			Type:        "int64",
			Must:        true,
			Description: "run period, unit: second.",
			Example:     "60",
		},
		&Template{
			Name:        "ExpiredAt",
			Field:       "expired_at",
			Type:        "int64",
			Must:        false,
			Description: "expired timestamp, 0: never expired.",
			Example:     "0",
		},
		&Template{
			Name:        "MaxRetries",
			Field:       "max_retries",
			Type:        "uint8",
			Must:        false,
			Description: "retry times, 0: unlimited.",
			Example:     "0",
		},
		&Template{
			Name:        "MaxRuns",
			Field:       "max_runs",
			Type:        "uint64",
			Must:        false,
			Description: "max permit run, 0: unlimited.",
			Example:     "0",
		},
	}

	// append zap config templates.
	templates = append(templates, GetZapConfigTemplate(ctx)...)

	return templates
}

// Get identifier of runner.
func (t *Task) GetId() string {
	return t.Tid
}

// Get Runner action Unix time.
func (t *Task) GetActionTime() int64 {
	return t.ActionAt
}

// Get Runner State.
func (t *Task) GetState() State {
	return t.Status
}

// Initialize runner with context, An error occurred will not be call Run function.
// will calc task md5 set into task tid, and call Created function.
func (t *Task) Init(ctx context.Context) error {
	// new logger.
	t.NewLogger(ctx)
	// set task tid.
	t.SetTid()
	// task created.
	t.Created()
	// task set init.
	t.Status = Init

	return nil
}

// Run with context, Return an error if some wrong.
func (t *Task) Run(ctx context.Context) error {
	// start.
	t.Start()

	return nil
}

// Finalizer with error and stat data.
func (t *Task) Finalizer(err error) {
	// stat data.
	defer t.stat()

	switch t.Status {
	case Canceled:
		break
	case Stopped:
		break
	case Retrying:
		break
	case Succeed, Failed:
		t.UpdateActionTime()
	default:
		break
	}
}

// Stop runner.
func (t *Task) Stop() error {
	if t.Status != Running {
		return NotRunErr
	}
	t.Status = Stopped
	return nil
}

// Cancel Runner.
func (t *Task) Cancel() {
	t.Status = Canceled
}

// Check Runner is effective, An error occurred will not be call Run function..
func (t *Task) Check() error {
	if t.Period == 0 {
		t.Period = DefaultPeriod
	}
	// value is effective.
	if len(t.Tid) == 0 {
		return ParamIllegal.WithDetail("Tid is not set")
	}
	if t.ActionAt <= 0 {
		return ParamIllegal.WithDetail("ActionAt is not set")
	}
	// is expired?
	if (time.Now().Unix() > t.ExpiredAt) && t.ExpiredAt > 0 {
		return RunnerExpired
	}

	return nil
}

// Runner Expired status, when false Scheduler will put into Queuer.
func (t *Task) Expired() bool {
	// not expired.
	if (time.Now().Unix() < t.ExpiredAt) || t.ExpiredAt <= 0 {
		t.Status = Queued
		return false
	}
	// update status.
	t.Status = Expired

	return true
}

// New task.
func NewTask(name string, opts ...TaskOption) Task {
	t := Task{
		Status:     Created,
		MaxRetries: 5,
	}

	// call task init.
	t.Init(context.TODO())

	// range options.
	for _, opt := range opts {
		opt.Apply(&t)
	}

	return t
}
