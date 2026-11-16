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

	deadLettered       *prometheus.CounterVec
	deadLetterFailures *prometheus.CounterVec
	retentionDeleted   *prometheus.CounterVec
	retentionErrors    prometheus.Counter
	partitionsDropped  prometheus.Counter
	retentionLastRun   prometheus.Gauge

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
		deadLettered: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_processor_dead_lettered_total",
			Help: "Poison records copied to a dead-letter topic, by source topic.",
		}, []string{"topic"}),
		deadLetterFailures: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_processor_dead_letter_failures_total",
			Help: "Poison records skipped without reaching a dead-letter topic, by source topic.",
		}, []string{"topic"}),
		retentionDeleted: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_processor_retention_rows_deleted_total",
			Help: "Rows removed by retention, by table.",
		}, []string{"table"}),
		partitionsDropped: f.NewCounter(prometheus.CounterOpts{
			Name: "connectivity_processor_retention_partitions_dropped_total",
			Help: "Hourly telemetry partitions dropped by retention.",
		}),
		retentionErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "connectivity_processor_retention_errors_total",
			Help: "Retention passes that failed.",
		}),
		retentionLastRun: f.NewGauge(prometheus.GaugeOpts{
			Name: "connectivity_processor_retention_last_success_timestamp_seconds",
			Help: "When retention last completed a pass on this replica (Unix time).",
		}),
		bundleIndexLatency: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "connectivity_processor_bundle_index_latency_seconds",
			Help:    "Delay between a device collecting a log bundle and the processor indexing it.",
			Buckets: []float64{1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800, 3600},
		}),
	}
}
