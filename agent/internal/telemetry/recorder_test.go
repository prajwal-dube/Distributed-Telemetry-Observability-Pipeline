package telemetry

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBoundedNonblockingRecorder(t *testing.T) {
	r := New("http://127.0.0.1:1", "demo", 2)
	s := Span{TraceID: ID(16), SpanID: ID(8)}
	if !r.Record(s) || !r.Record(s) || r.Record(s) {
		t.Fatal("incorrect bounded queue")
	}
	if r.Dropped.Load() != 1 {
		t.Fatal("drop not visible")
	}
}
func BenchmarkRecord(b *testing.B) {
	r := New("unused", "demo", 1024)
	s := Span{TraceID: ID(16), SpanID: ID(8)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Record(s)
		if i%1024 == 1023 {
			for j := 0; j < 1024; j++ {
				<-r.queue
			}
		}
	}
}
func TestSnapshot(t *testing.T) {
	r := New("unused", "demo", 1)
	time.Sleep(time.Millisecond)
	s := r.Snapshot()
	if s.HeapBytes == 0 || s.PeakRSSBytes <= 0 {
		t.Fatal("missing process metrics")
	}
}

func TestRetryReusesBatchID(t *testing.T) {
	var requests atomic.Int64
	var firstID string
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		var batch Batch
		json.NewDecoder(q.Body).Decode(&batch)
		mu.Lock()
		if firstID == "" {
			firstID = batch.BatchID
		} else if firstID != batch.BatchID {
			t.Error("retry changed batch ID")
		}
		mu.Unlock()
		if requests.Add(1) == 1 {
			w.WriteHeader(500)
		} else {
			w.WriteHeader(202)
		}
	}))
	defer server.Close()
	r := New(server.URL, "demo", 2)
	r.Record(Span{TraceID: ID(16), SpanID: ID(8), Service: "checkout", Operation: "retry", StartMS: time.Now().UnixMilli(), DurationMS: 1, Status: 200})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	deadline := time.Now().Add(3 * time.Second)
	for r.Delivered.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	if r.Delivered.Load() != 1 || requests.Load() != 2 || r.Dropped.Load() != 0 {
		t.Fatal("retry did not deliver exactly once to accepting response")
	}
}
