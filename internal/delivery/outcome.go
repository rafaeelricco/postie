package delivery

// Outcome is how delivery of one record ended. Every outcome is terminal: the
// record's offset may be committed once one is reached. The zero value is not
// an outcome and only accompanies an error.
type Outcome int

const (
	// Delivered means the destination acknowledged the event.
	Delivered Outcome = iota + 1
	// Filtered means the destination's filter excluded the event, so nothing
	// was sent.
	Filtered
	// Skipped means the destination rejected the event for good ("keep_going").
	// A skip is recorded durably before the offset is committed.
	Skipped
)

// String is the outcome name used in activity entries.
func (o Outcome) String() string {
	switch o {
	case Delivered:
		return "delivered"
	case Filtered:
		return "filtered"
	case Skipped:
		return "skipped"
	default:
		return ""
	}
}
