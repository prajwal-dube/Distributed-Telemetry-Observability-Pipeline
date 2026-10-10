package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"telemetryagent/internal/telemetry"
	"time"
)

func env(k, d string) string {
	if s := os.Getenv(k); s != "" {
		return s
	}
	return d
}
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	capacity, _ := strconv.Atoi(env("AGENT_BUFFER", "10000"))
	if capacity < 1 {
		log.Fatal("positive buffer required")
	}
	r := telemetry.New(env("INGEST_URL", "http://localhost:8082/ingest"), env("API_TOKEN", "demo-token"), capacity)
	r.Enabled = env("TELEMETRY_ENABLED", "1") == "1"
	go r.Run(ctx)
	m := http.NewServeMux()
	m.HandleFunc("GET /health", func(w http.ResponseWriter, q *http.Request) { w.Write([]byte("ok")) })
	m.HandleFunc("GET /stats", func(w http.ResponseWriter, q *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"recorded": r.Recorded.Load(), "dropped": r.Dropped.Load(), "delivered": r.Delivered.Load(), "batches": r.Batches.Load(), "send_failures": r.Failures.Load()})
	})
	m.HandleFunc("GET /demo", func(w http.ResponseWriter, q *http.Request) {
		trace := telemetry.ID(16)
		root := telemetry.ID(8)
		now := time.Now().UnixMilli()
		r.Record(telemetry.Span{trace, root, "", "checkout", "POST /checkout", now, 35, 200})
		r.Record(telemetry.Span{trace, telemetry.ID(8), root, "inventory", "reserve", now + 2, 8, 200})
		r.Record(telemetry.Span{trace, telemetry.ID(8), root, "payments", "authorize", now + 12, 20, 200})
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"trace_id": trace})
	})
	srv := http.Server{Addr: env("LISTEN_ADDR", ":8083"), Handler: m, ReadHeaderTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second}
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		srv.Shutdown(c)
	}()
	log.Printf("agent %s", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
