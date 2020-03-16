package timeout

import (
	"time"
)

// WithTimeout executes a blocking function with a timeout delay
func WithTimeout(timeout time.Duration, f func() interface{}) interface{} {
	done := make(chan interface{}, 1)

	var res interface{}

	go func() {
		res = f()
		done <- res
	}()

	select {
	case _ = <-done:
		return res
	case <-time.After(timeout):
		return nil
	}
}
