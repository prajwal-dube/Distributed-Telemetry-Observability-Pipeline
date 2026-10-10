package telemetry

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"
)

type Span struct {
	TraceID    string  `json:"trace_id"`
	SpanID     string  `json:"span_id"`
	ParentID   string  `json:"parent_id,omitempty"`
	Service    string  `json:"service"`
	Operation  string  `json:"operation"`
	StartMS    int64   `json:"start_ms"`
	DurationMS float64 `json:"duration_ms"`
	Status     int     `json:"status"`
}
type Runtime struct {
	HeapBytes    uint64  `json:"heap_bytes"`
	PeakRSSBytes int64   `json:"peak_rss_bytes"`
	CPUCores     float64 `json:"cpu_cores"`
}
type Batch struct {
	BatchID string  `json:"batch_id"`
	Spans   []Span  `json:"spans"`
	Runtime Runtime `json:"runtime"`
}
type Recorder struct {
	queue                                           chan Span
	Recorded, Dropped, Delivered, Batches, Failures atomic.Uint64
	client                                          *http.Client
	URL, Token                                      string
	Enabled                                         bool
	lastCPU                                         float64
	lastWall                                        time.Time
}

func ID(bytes int) string {
	b := make([]byte, bytes)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
func New(url, token string, capacity int) *Recorder {
	return &Recorder{queue: make(chan Span, capacity), client: &http.Client{Timeout: 2 * time.Second}, URL: url, Token: token, Enabled: true, lastWall: time.Now()}
}

// Record never waits for a network call or queue capacity. Caller owns span construction.
func (r *Recorder) Record(s Span) bool {
	if !r.Enabled {
		return true
	}
	select {
	case r.queue <- s:
		r.Recorded.Add(1)
		return true
	default:
		r.Dropped.Add(1)
		return false
	}
}
func (r *Recorder) Snapshot() Runtime {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	var usage syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	cpu := float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
	now := time.Now()
	cores := (cpu - r.lastCPU) / now.Sub(r.lastWall).Seconds()
	r.lastCPU = cpu
	r.lastWall = now
	return Runtime{m.HeapAlloc, usage.Maxrss * 1024, cores}
}
func (r *Recorder) Run(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	buffer := make([]Span, 0, 500)
	flush := func() {
		if len(buffer) == 0 {
			return
		}
		batch := Batch{ID(16), buffer, r.Snapshot()}
		body, err := json.Marshal(batch)
		if err != nil {
			r.Dropped.Add(uint64(len(buffer)))
			buffer = buffer[:0]
			return
		}
		success := false
		for attempt := 0; attempt < 5; attempt++ {
			req, err := http.NewRequestWithContext(ctx, "POST", r.URL, bytes.NewReader(body))
			if err != nil {
				break
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+r.Token)
			resp, err := r.client.Do(req)
			if err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode == 202 {
					success = true
					break
				}
				if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != 429 {
					break
				}
			}
			r.Failures.Add(1)
			select {
			case <-ctx.Done():
				break
			case <-time.After(time.Duration(attempt+1) * 100 * time.Millisecond):
			}
		}
		if success {
			r.Delivered.Add(uint64(len(buffer)))
			r.Batches.Add(1)
		} else {
			r.Dropped.Add(uint64(len(buffer)))
		}
		buffer = buffer[:0]
	}
	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case s := <-r.queue:
			buffer = append(buffer, s)
			if len(buffer) == 500 {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}
