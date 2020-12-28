package logger

// Log interface used for logging
type Log interface {
	Debugf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}
