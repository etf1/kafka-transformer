package timeout_test

import (
	"testing"
	"time"

	"github.com/etf1/kafka-transformer/internal/timeout"
)

func TestWithoutTimeout(t *testing.T) {

	f := func() interface{} {
		time.Sleep(2 * time.Second)
		return true
	}

	res := timeout.WithTimeout(5*time.Second, f)

	if res == nil {
		t.Errorf("unexpected result, should NOT be nil, got %v", res)
	}

}

func TestWithTimeout(t *testing.T) {

	f := func() interface{} {
		time.Sleep(5 * time.Second)
		return true
	}

	res := timeout.WithTimeout(2*time.Second, f)

	if res != nil {
		t.Errorf("unexpected result, should be nil, got %v", res)
	}

}
