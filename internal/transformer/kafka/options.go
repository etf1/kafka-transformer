package kafka

import (
	"go.opentelemetry.io/otel/trace"
)

type optionsConfig struct {
	tracerProvider trace.TracerProvider
}

type Option func(c *optionsConfig)

// WithTracerProvider allows to specify an OpenTelemetry TracerProvider.
func WithTracerProvider(tracerProvider trace.TracerProvider) Option {
	return func(c *optionsConfig) {
		c.tracerProvider = tracerProvider
	}
}
