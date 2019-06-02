package scheduler

// Defined state.
type State uint

// State enum.
const (
	Created State = 1 << iota
	Init
	Queued
	Running
	Retrying
	Expired
	Stopped
	Canceled
	Succeed
	Failed
)

// Implement Stringer interface.
func (s State) String() string {
	switch s {
	case Created:
		return "Created"
	case Init:
		return "Init"
	case Queued:
		return "Queued"
	case Running:
		return "Running"
	case Retrying:
		return "Retrying"
	case Expired:
		return "Expired"
	case Stopped:
		return "Stopped"
	case Canceled:
		return "Canceled"
	case Succeed:
		return "Succeed"
	case Failed:
		return "Failed"
	}

	return "Undefined"
}

// Get State value.
func (s State) Value() uint {
	return uint((s))
}
