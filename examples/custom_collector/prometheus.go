package main

import (
	"log"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/etf1/kafka-transformer/pkg/instrument"
	"github.com/prometheus/client_golang/prometheus"
)

type promCollector struct {
	name                                                                                                                               string
	consumeDurationGauge, transformDurationGauge, produceDurationGauge, projectDurationGauge, overallDurationGauge                     *prometheus.GaugeVec
	consumeCounter, transformCounter, produceCounter, projectCounter, overallCounter                                                   *prometheus.CounterVec
	consumeDurationHistogram, transformDurationHistogram, produceDurationHistogram, projectDurationHistogram, overallDurationHistogram *prometheus.HistogramVec
}

// NewCollector create a prometheus collector
func NewCollector(name string) instrument.Collector {
	pc := promCollector{
		name: name,
		// gauges
		consumeDurationGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "kafka_transformer",
				Name:      "consume_gauge_duration_seconds",
				Help:      "Current consume duration in seconds",
			},
			[]string{"name", "status"},
		),
		transformDurationGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "kafka_transformer",
				Name:      "transform_gauge_duration_seconds",
				Help:      "Current transform duration in seconds",
			},
			[]string{"name", "status"},
		),
		produceDurationGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "kafka_transformer",
				Name:      "produce_gauge_duration_seconds",
				Help:      "Current produce duration in seconds",
			},
			[]string{"name", "status"},
		),
		projectDurationGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "kafka_transformer",
				Name:      "project_gauge_duration_seconds",
				Help:      "Current project duration in seconds",
			},
			[]string{"name", "status"},
		),
		overallDurationGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "kafka_transformer",
				Name:      "overall_gauge_duration_seconds",
				Help:      "Current overall duration in seconds",
			},
			[]string{"name", "status"},
		),
		// counters
		consumeCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka_transformer",
				Name:      "consume_counter_total",
				Help:      "The number of consumed messages from a kafka topic",
			},
			[]string{"name", "status"},
		),
		transformCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka_transformer",
				Name:      "transform_counter_total",
				Help:      "The number of transformed messages",
			},
			[]string{"name", "status"},
		),
		produceCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka_transformer",
				Name:      "produce_counter_total",
				Help:      "The number of produced messages to a kafka topic",
			},
			[]string{"name", "status"},
		),
		projectCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka_transformer",
				Name:      "project_counter_total",
				Help:      "The number of projected messages",
			},
			[]string{"name", "status"},
		),
		overallCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka_transformer",
				Name:      "overall_counter_total",
				Help:      "The number of messages processed by the transformer from consumption to projection",
			},
			[]string{"name", "status"},
		),
		// histograms
		consumeDurationHistogram: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "kafka_transformer",
				Name:      "consume_hist_duration_seconds",
				Help:      "The consumption duration in seconds",
			},
			[]string{"name", "status"},
		),
		transformDurationHistogram: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "kafka_transformer",
				Name:      "transform_hist_duration_seconds",
				Help:      "The transformed duration in seconds",
			},
			[]string{"name", "status"},
		),
		produceDurationHistogram: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "kafka_transformer",
				Name:      "produce_hist_duration_seconds",
				Help:      "The produced duration in seconds",
			},
			[]string{"name", "status"},
		),
		projectDurationHistogram: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "kafka_transformer",
				Name:      "project_hist_duration_seconds",
				Help:      "The projection duration in seconds",
			},
			[]string{"name", "status"},
		),
		overallDurationHistogram: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "kafka_transformer",
				Name:      "overall_hist_duration_seconds",
				Help:      "The overall duration in seconds",
			},
			[]string{"name", "status"},
		),
	}

	prometheus.MustRegister(pc.consumeDurationGauge, pc.transformDurationGauge, pc.produceDurationGauge, pc.projectDurationGauge, pc.overallDurationGauge)
	prometheus.MustRegister(pc.consumeCounter, pc.transformCounter, pc.produceCounter, pc.projectCounter, pc.overallCounter)
	prometheus.MustRegister(pc.consumeDurationHistogram, pc.transformDurationHistogram, pc.produceDurationHistogram, pc.projectDurationHistogram, pc.overallDurationHistogram)

	return &pc
}

func (p promCollector) Before(message *confluent.Message, action instrument.Action, start time.Time) {
	log.Printf("PromCollector Before: %v %v %v", message, action, start)
}

func (p promCollector) After(message *confluent.Message, action instrument.Action, err error, start time.Time) {
	now := time.Now()

	log.Printf("PromCollector After: %v %v %v %v", message, action, err, start)

	status := "success"
	if err != nil {
		status = "failed"
	}

	var (
		gauge   *prometheus.GaugeVec
		counter *prometheus.CounterVec
		hist    *prometheus.HistogramVec
	)

	switch action {
	case instrument.KafkaConsumerConsume:
		gauge = p.consumeDurationGauge
		counter = p.consumeCounter
		hist = p.consumeDurationHistogram
	case instrument.TransformerTransform:
		gauge = p.transformDurationGauge
		counter = p.transformCounter
		hist = p.transformDurationHistogram
	case instrument.KafkaProducerProduce:
		gauge = p.produceDurationGauge
		counter = p.produceCounter
		hist = p.produceDurationHistogram
	case instrument.ProjectorProject:
		gauge = p.projectDurationGauge
		counter = p.projectCounter
		hist = p.projectDurationHistogram
	case instrument.OverallTime:
		gauge = p.overallDurationGauge
		counter = p.overallCounter
		hist = p.overallDurationHistogram
	}

	if gauge != nil {
		gauge.WithLabelValues(p.name, status).Set(now.Sub(start).Seconds())
	}
	if counter != nil {
		counter.WithLabelValues(p.name, status).Inc()
	}
	if hist != nil {
		hist.WithLabelValues(p.name, status).Observe(now.Sub(start).Seconds())
	}

}
