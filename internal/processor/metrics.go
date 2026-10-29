package processor

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	records       *prometheus.CounterVec
	samples       prometheus.Counter
	retries       *prometheus.CounterVec
	duration      *prometheus.HistogramVec
	ingestToStore prometheus.Histogram
	commitErrors  prometheus.Counter

	bundleIndexLatency prometheus.Histogram
}

func newMetrics(reg prometheus.Registerer) *metrics {
	f := promauto.With(reg)
	return &metrics{
		records: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_processor_records_total",
			Help: "Kafka records handled, by topic and result (stored, duplicate, poison).",
		}, []string{"topic", "result"}),
		samples: f.NewCounter(prometheus.CounterOpts{
			Name: "connectivity_processor_samples_stored_total",
			Help: "Telemetry samples written to Postgres.",
		}),
		retries: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_processor_retries_total",
			Help: "Record processing attempts that failed and were retried.",
		}, []string{"topic"}),
		duration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "connectivity_processor_record_duration_seconds",
			Help:    "Time to process one record, including the database write.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
		}, []string{"topic"}),
		ingestToStore: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "connectivity_processor_ingest_to_store_seconds",
			Help:    "Delay between ingestion accepting a batch and the processor storing it.",
			Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300},
		}),
		commitErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "connectivity_processor_commit_errors_total",
			Help: "Failed Kafka offset commits.",
		}),
		bundleIndexLatency: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "connectivity_processor_bundle_index_latency_seconds",
			Help:    "Delay between a device collecting a log bundle and the processor indexing it.",
			Buckets: []float64{1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800, 3600},
		}),
	}
}
