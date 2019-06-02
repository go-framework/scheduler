package scheduler

import (
	"time"
)

// An Option configures a Schedule.
type ScheduleOption interface {
	Apply(*Schedule)
}

// ScheduleOptionFunc wraps a func so it satisfies the Option interface.
type ScheduleOptionFunc func(*Schedule)

func (f ScheduleOptionFunc) Apply(s *Schedule) {
	f(s)
}

// With MaxConcurrent option.
func WithMaxConcurrentOption(n uint64) ScheduleOption {
	return ScheduleOptionFunc(func(s *Schedule) {
		s.MaxConcurrent = n
	})
}

// With EnabledStat option.
func WithEnableStatOption(enable bool) ScheduleOption {
	return ScheduleOptionFunc(func(s *Schedule) {
		s.EnableStat = enable
	})
}

// With EnablePrintStat option.
func WithEnablePrintStatOption(enable bool) ScheduleOption {
	return ScheduleOptionFunc(func(s *Schedule) {
		s.EnablePrintStat = enable
	})
}

// With PrintStatInterval option.
func WithPrintStatIntervalOption(d time.Duration) ScheduleOption {
	return ScheduleOptionFunc(func(s *Schedule) {
		s.PrintStatInterval = d
	})
}
