package processor

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	return rec
}

func attr(s sdktrace.ReadOnlySpan, key string) attribute.Value {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value
		}
	}
	return attribute.Value{}
}

func TestRecordSpanJoinsTheIngestTrace(t *testing.T) {
	spans := recordSpans(t)
	const ingestTrace = "4bf92f3577b34da6a3ce929d0e0e4736"
	p := newTestProcessor(&fakeStore{failures: 2})
	rec := telemetryRecord(t, "b1")
	rec.Headers = []kgo.RecordHeader{{Key: "traceparent", Value: []byte("00-" + ingestTrace + "-00f067aa0ba902b7-01")}}

	if err := p.handleWithRetry(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	ended := spans.Ended()
	if len(ended) != 1 {
		t.Fatalf("want one span per record however many attempts, got %d", len(ended))
	}
	s := ended[0]
	if s.Name() != "process telemetry.raw" || s.SpanKind() != trace.SpanKindConsumer {
		t.Errorf("span %q kind %v", s.Name(), s.SpanKind())
	}
	if s.SpanContext().TraceID().String() != ingestTrace || s.Parent().SpanID().String() != "00f067aa0ba902b7" {
		t.Errorf("span is not a child of the ingest API's producer span: trace %s parent %s",
			s.SpanContext().TraceID(), s.Parent().SpanID())
	}
	if got := attr(s, "outcome").AsString(); got != "stored" {
		t.Errorf("outcome %q", got)
	}
	if got := attr(s, "batch.id").AsString(); got != "b1" {
		t.Errorf("batch.id %q", got)
	}
	retries := 0
	for _, ev := range s.Events() {
		if ev.Name == "retry" {
			retries++
		}
	}
	if retries != 2 {
		t.Errorf("two failed attempts should be two retry events, got %d", retries)
	}
	if s.Status().Code == codes.Error {
		t.Error("a record stored after retries is not a failed span")
	}
}

func TestPoisonRecordSpanIsAnError(t *testing.T) {
	spans := recordSpans(t)
	p := newTestProcessor(&fakeStore{})
	rec := &kgo.Record{Topic: "telemetry.raw", Value: []byte("not json")}
	if err := p.handleWithRetry(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	s := spans.Ended()[0]
	if s.Status().Code != codes.Error || attr(s, "outcome").AsString() != "poison" {
		t.Errorf("poison record: status %v, outcome %q", s.Status(), attr(s, "outcome").AsString())
	}
	if s.Parent().IsValid() {
		t.Error("a record without trace headers should start its own trace")
	}
}
