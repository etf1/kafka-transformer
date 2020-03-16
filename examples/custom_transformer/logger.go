package main

import log "github.com/sirupsen/logrus"

// custom Logger
type myLogger struct{}

func (m myLogger) Debugf(format string, args ...interface{}) {
	log.WithField("app", "custom-transformer").Debugf(format, args...)
}
func (m myLogger) Errorf(format string, args ...interface{}) {
	log.WithField("app", "custom-transformer").Errorf(format, args...)
}
