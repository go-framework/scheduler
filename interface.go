package scheduler

import (
	"context"

	"go.uber.org/zap"
)

// Template defined for compose a JSON/YAML etc., format data to new Runner .
type Template struct {
	// Name of field.
	Name string `json:"name" yaml:"name"`
	// Field.
	Field string
	// Type name of Golang.
	Type string `json:"type" yaml:"type"`
	// Must config.
	Must bool `json:"must" yaml:"must"`
	// Description
	Description string `json:"description" yaml:"description"`
	// Example config value.
	Example string `json:"example" yaml:"example"`
}

// Runner defined the scheduler run interface.
type Runner interface {
	// Get identifier of Runner.
	GetId() string
	// Get name of Runner.
	GetName() string
	// Get Runner action Unix time.
	GetActionTime() int64
	// Get Runner State.
	GetState() State

	// Initialize Runner with context, An error occurred will not be call Run function.
	Init(context.Context) error
	// Check Runner is effective, An error occurred will not be call Run function.
	Check() error
	// Run with context, Return an error if some wrong.
	Run(context.Context) error
	// Finalizer with error.
	Finalizer(error)
	// Runner Reusable, when true Scheduler will put into Queuer.
	Reusable() bool

	// Stop Runner.
	Stop() error
	// Cancel Runner.
	Cancel()

	// Get type of Runner.
	GetType() string
	// Get template of Runner.
	GetTemplate(context.Context) []*Template
	// New Runner.
	New() Runner
}

// Realtime Runner run Store interface.
type RealtimeStore interface {
	// Initialize Store with context.
	Init(context.Context) error
	// Get Runner from Store with string id.
	Get(string) (Runner, error)
	// Put Runner into Store.
	Put(Runner) error
	// Delete Runner in Store.
	Delete(Runner) error
	// List Runners from Store with context and record count and offset.
	List(ctx context.Context, count, offset uint64) ([]Runner, error)
}

// Queuer interface.
type Queuer interface {
	// Register Runner into Queuer.
	Register(Runner Runner)
	// Initialize Queuer with context.
	Init(context.Context) error
	// Push Runners into the Queuer.
	Push(...Runner) error
	// Pop Runners with context and record count and offset.
	Pop(ctx context.Context, count, offset uint64) ([]Runner, error)
	// Remove Runner from the Queuer.
	Remove(Runner) error
	// List all Runner in Queuer with count and offset.
	List(ctx context.Context, count, offset uint64) ([]Runner, error)
	// Length of Queuer.
	Length() int64
	// Waiting length of Queuer.
	Waiting() int64
}

// Scheduler interface.
// scheduler call Runner sequence is Init -> Check -> Run-> Finalizer -> Expired.
type Scheduler interface {
	// Get Context.
	GetContext() context.Context
	// Get Queuer.
	GetQueuer() Queuer
	// Get Logger.
	GetLogger() *zap.Logger
	// Get error chan.
	Error() <-chan error

	// Initialize scheduler with context.
	Init(context.Context) error
	// Run the scheduler.
	Run() error
	// Stop run the scheduler.
	Stop() error

	// Stop a Runner with string id.
	StopRunner(string) error
	// Remove a Runner with string id.
	RemoveRunner(string) error
}

// Progress interface.
type Progresser interface {
	// Progress.
	Progress(event string, data interface{})
}
