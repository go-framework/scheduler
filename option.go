package scheduler

// An Option configures a Schedule.
type ScheduleOption interface {
	Apply(*Schedule)
}

// ScheduleOptionFunc wraps a func so it satisfies the Option interface.
type ScheduleOptionFunc func(*Schedule)

func (f ScheduleOptionFunc) Apply(e *Schedule) {
	f(e)
}

// An Option configures a Task.
type TaskOption interface {
	Apply(*Task)
}

// TaskOptionFunc wraps a func so it satisfies the Option interface.
type TaskOptionFunc func(*Task)

func (f TaskOptionFunc) Apply(t *Task) {
	f(t)
}

// With Task action time.
func WithActionTime(timestamp int64) TaskOption {
	return TaskOptionFunc(func(t *Task) {
		t.ActionAt = timestamp
	})
}
