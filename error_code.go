package scheduler

import (
	"github.com/go-framework/errors"
)

// Defined Error type.
type Error string

// Implement error interface.
func (e Error) Error() string {
	return string(e)
}

// Implement Message interface.
func (e Error) Message() string {
	return string(e)
}

// Return a new error with detail.
func (e Error) WithDetail(detail interface{}) error {
	return errors.NewCode(e, detail)
}

// Defined errors.
const (
	ParamIllegal    Error = "param illegal"
	ContextNilErr   Error = "context is nil"
	NotRunErr       Error = "not start run"
	RepeatRunErr    Error = "repeat run error"
	ManualStopErr   Error = "manual stop"
	ManualCancelErr Error = "manual cancel"
	RunnerExpired   Error = "runner expired"
	QueuerPopErr    Error = "queuer pop error"
	InvalidRunner   Error = "invalid runner"
)
