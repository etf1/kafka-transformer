package instrument

import "time"

// TimeHolder is used to store start time for consume and project.
// It is used in the Message.Opaque field
type TimeHolder struct {
	ConsumeStart time.Time
	ProjectStart time.Time
	// Original Opaque of the message, if any.
	Opaque interface{}
}
