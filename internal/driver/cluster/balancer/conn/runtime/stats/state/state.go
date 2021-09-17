package state

type State int8

const (
	Offline State = iota - 2
	Banned
	Unknown
	Online
)

func (s State) String() string {
	switch s {
	case Online:
		return "online"
	case Offline:
		return "offline"
	case Banned:
		return "banned"
	default:
		return "unknown"
	}
}