package delivery

type Outcome int

const (
	Delivered Outcome = iota + 1
	Filtered
	Skipped
)
