package otel

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

var (
	tracerProviderSync sync.Once
	tracerProvider     trace.TracerProvider
)

// GetTracerProvider initializes a gRPC connection to the OpenTelemetry Collector
// and returns a TracerProvider with this connection.
func GetTracerProvider(ctx context.Context, endpoint, appName, appVersion string) trace.TracerProvider {
	tracerProviderSync.Do(func() {
		traceExporter, err := otlptracegrpc.New(
			ctx,
			otlptracegrpc.WithInsecure(),
			otlptracegrpc.WithEndpoint(endpoint),
			otlptracegrpc.WithDialOption(
				grpc.WithBlock(),
			),
		)
		if err != nil {
			panic(err)
		}

		tracerProvider = sdktrace.NewTracerProvider(
			sdktrace.WithResource(resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceNameKey.String(appName),
				semconv.ServiceVersionKey.String(appVersion),
			)),
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
			sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(traceExporter)),
		)

		otel.SetTracerProvider(tracerProvider)
		otel.SetTextMapPropagator(propagation.TraceContext{})
	})

	return tracerProvider
}
