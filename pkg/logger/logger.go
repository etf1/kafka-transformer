package logger

import (
	"log"
	"os"
)

// Log interface used for logging
type Log interface {
	Debugf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

type defaultLogger struct {
	out *log.Logger
	err *log.Logger
}

// DefaultLogger is the default implementation of Log stdout/stderr
func DefaultLogger() Log {
	return defaultLogger{
		out: log.New(os.Stdout, "", log.LstdFlags),
		err: log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Debugf writes a debug log
func (d defaultLogger) Debugf(format string, args ...interface{}) {
	d.out.Printf(format, args...)
}

// Debugf writes an error log
func (d defaultLogger) Errorf(format string, args ...interface{}) {
	d.err.Printf(format, args...)
}
