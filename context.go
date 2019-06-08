package scheduler

import (
	"context"
	"time"

	"github.com/go-framework/zap"
)

// duration context key.
type durationContextKey struct{}

// New duration context.
func NewDurationContext(ctx context.Context, duration time.Duration) context.Context {
	return context.WithValue(ctx, durationContextKey{}, duration)
}

// Get duration from context.
func DurationFromContext(ctx context.Context) (time.Duration, bool) {
	duration, ok := ctx.Value(durationContextKey{}).(time.Duration)
	return duration, ok
}

// do function context key.
type doFunctionKey struct{}

// Do function handler.
type DoFuncHandler func()

// New function context.
func NewDoFunctionContext(ctx context.Context, f DoFuncHandler) context.Context {
	return context.WithValue(ctx, doFunctionKey{}, f)
}

// Get function from context.
func DoFunctionFromContext(ctx context.Context) (DoFuncHandler, bool) {
	f, ok := ctx.Value(doFunctionKey{}).(DoFuncHandler)
	return f, ok
}

// queuer context key.
type queuerContextKey struct{}

// New Queuer context.
func NewQueuerContext(ctx context.Context, queuer Queuer) context.Context {
	return context.WithValue(ctx, queuerContextKey{}, queuer)
}

// Get Queuer from context.
func QueuerFromContext(ctx context.Context) (Queuer, bool) {
	queuer, ok := ctx.Value(queuerContextKey{}).(Queuer)
	return queuer, ok
}

// zap Config context key.
type zapConfigContextKey struct{}

// New Zap Config context.
func NewZapConfigContext(ctx context.Context, config *zap.Config) context.Context {
	return context.WithValue(ctx, zapConfigContextKey{}, config)
}

// Get Zap Config from context.
func ZapConfigFromContext(ctx context.Context) (*zap.Config, bool) {
	config, ok := ctx.Value(zapConfigContextKey{}).(*zap.Config)
	return config, ok
}

// zap Name context key.
type zapNameContextKey struct{}

// New Zap Name context.
func NewZapNameContext(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, zapNameContextKey{}, name)
}

// Get Zap Name from context.
func ZapNameFromContext(ctx context.Context) (string, bool) {
	name, ok := ctx.Value(zapNameContextKey{}).(string)
	return name, ok
}

// zap Option context key.
type zapOptionContextKey struct{}

// New Zap Option context.
func NewZapOptionContext(ctx context.Context, opts ...zap.Option) context.Context {
	return context.WithValue(ctx, zapOptionContextKey{}, opts)
}

// Get Zap Option from context.
func ZapOptionFromContext(ctx context.Context) ([]zap.Option, bool) {
	opts, ok := ctx.Value(zapOptionContextKey{}).([]zap.Option)
	return opts, ok
}

// runner zap Config context key.
type runnerZapConfigContextKey struct{}

// New runner Zap Config context.
func NewRunnerZapConfigContext(ctx context.Context, config *zap.Config) context.Context {
	return context.WithValue(ctx, runnerZapConfigContextKey{}, config)
}

// Get Zap Config from context.
func RunnerZapConfigFromContext(ctx context.Context) (*zap.Config, bool) {
	config, ok := ctx.Value(runnerZapConfigContextKey{}).(*zap.Config)
	return config, ok
}

// runner zap Option context key.
type runnerZapOptionContextKey struct{}

// New runner Zap Option context.
func NewRunnerZapOptionContext(ctx context.Context, opts ...zap.Option) context.Context {
	return context.WithValue(ctx, runnerZapOptionContextKey{}, opts)
}

// Get runner Zap Option from context.
func RunnerZapOptionFromContext(ctx context.Context) ([]zap.Option, bool) {
	opts, ok := ctx.Value(runnerZapOptionContextKey{}).([]zap.Option)
	return opts, ok
}
