package logger

import (
	"log"
	"os"

	"github.com/etf1/kafka-transformer/pkg/logger"
)

// Stdout system out implementation of Logger.Log interface
type Stdout struct {
	out *log.Logger
	err *log.Logger
}

// StdoutLogger is the default implementation of Log stdout/stderr
func StdoutLogger() logger.Log {
	return Stdout{
		out: log.New(os.Stdout, "", log.LstdFlags),
		err: log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Debugf writes a debug log
func (s Stdout) Debugf(format string, args ...interface{}) {
	s.out.Printf(format, args...)
}

// Errorf writes an error log
func (s Stdout) Errorf(format string, args ...interface{}) {
	s.err.Printf(format, args...)
}
