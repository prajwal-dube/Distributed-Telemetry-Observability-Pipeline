# Telemetry semantics and reliability

## Agent and application impact

`agent/internal/telemetry.Recorder.Record` uses a nonblocking send into a bounded Go channel. It performs no network call and returns false immediately if the queue is full. The daemon's demo handler instruments a synthetic checkout span with inventory/payment child spans and returns a trace ID. It is a distributed-trace **simulator**, not automatic discovery/instrumentation of arbitrary existing microservices.

The Go recorder is reusable within a Go application. The daemon does not intercept all application CPU/HTTP activity; applications must instrument spans explicitly. Reported heap, peak resident memory and CPU are for the **agent process**, not remote services or the entire host. CPU is process CPU-time delta divided by wall time (cores); memory is runtime heap allocation and Linux Getrusage peak RSS.

One flush loop batches up to 500 spans every 500 ms, also flushing immediately when the batch reaches 500. Network delivery is performed outside application handlers. It uses a two-second HTTP deadline and at most five attempts, always reusing the same batch ID. Network failures can fill the 10,000-span channel; overflow and failed-batch abandonment increment visible dropped counters. Agent buffering is memory-only; a process crash can lose unsent spans, and shutdown does not guarantee full queue drain. The recorder is nonblocking, not lossless or zero-overhead. The daemon demo always reports HTTP 200 independently of queue acceptance, so inspect `/stats` for telemetry drops.

## Persistent ingestion and processing

API validates 1–1,000 span records per batch, finite bounded durations, identifiers, and a registered service belonging to the authenticated demo account. Metric labels use only registered service names and five HTTP status classes. Trace/span IDs and operation names never become metric labels. API token SHA-256 hashes, service ownership/team metadata, rules, sampled spans and alert/outbox records reside in PostgreSQL. This is API-key authentication for a private single-tenant demo, not a complete login/password/session system.

One Redis Lua admission script stores payload/digest/status and appends an outbox stream entry. Same ID/same body returns existing status; conflicting reuse returns 409. Relay publishes a Celery task containing only batch ID, then ACKs the stream entry. An interrupted publish/ACK can duplicate the task; abandoned stream entries are reclaimed after two seconds. Redis holds both the outbox (DB 0) and Celery broker (DB 1), with AOF always and noeviction.

Workers persist sampled trace spans with `(trace_id,span_id)` uniqueness, then atomically apply metric updates and mark the batch DONE with Redis Lua. A crash before aggregation can replay trace inserts. A crash after aggregation finds the DONE marker and does not double-count. Lua accumulator types are checked before histogram writes; scripts isolate competing operations but do not provide rollback for arbitrary runtime errors. Internal Redis key types must be controlled. The cross-store ordering supports tested worker restart windows; it is not a distributed transaction or arbitrary-failure data-loss proof.

Celery uses late ACK, reject-on-worker-loss, prefetch multiplier 1 and bounded error autoretries. Default Redis broker visibility timeout is 60 seconds; native recovery tests used five seconds for short tasks, but actual recovery took 91.423 seconds because restoration polling adds delay. Solo workers are separate processes, one task at a time each; scale processes for parallel batches. A task that exceeds its visibility timeout can be delivered twice, but batch aggregation is idempotent. Repeated exhausted task failures can leave a batch QUEUED; operators inspect logs and replay/fix it. There is no complete DLQ/terminal-batch-failure management UI.

## Rolling window and quantiles

Aggregation assigns every span the batch's server **ingestion time**, not its caller's start timestamp. Six completed 10-second histogram buckets cover a 60-second window and are evaluated every 10 seconds. The current partial bucket is excluded. This is an aligned sliding window, with up to 10 seconds of delay, not an exact continuously moving per-event window.

Latency bins in ms: .5, 1, 2, 5, 10, 25, 50, 100, 200, 500, 1,000, 2,000, 5,000, 600,000. Within each window, p99 is the smallest bin boundary containing the nearest-rank 99th-percentile sample: a conservative **bucket upper bound**, not an exact sorted p99. The bounded histogram state avoids storing/sorting every raw metric. Raw sampled trace spans retain their exact durations in PostgreSQL.

Prometheus scrapes cumulative classic histograms, status counters, complete-window mean/count/p99-upper-bound gauges and the latest agent runtime snapshot. `histogram_quantile` on histogram rates produces a separate interpolated estimate; Grafana clearly labels it as estimated. Prometheus stores the scraped aggregated time series, not every raw JSON batch, log line, or trace. No log-rotation functionality is implemented.

## Sampling, caching and cardinality

Trace sampling defaults to 1%, deterministically selected by trace ID, so all spans in a selected trace agree. Set `TRACE_SAMPLE_RATE=1` for a small complete-trace demo. Sampling reduces SQL writes; it can omit the trace you search for. Missing roots/parents, delayed spans and clock skew can yield incomplete timelines; this demo does not infer causality, validate complete trace graphs or calculate critical-path latency.

Service metadata uses a 30-second Redis cache. Primary keys and supporting trace/rule/alert/outbox indexes are defined in `schema.sql`. SQL remains authoritative; manual profile changes can be stale until TTL expires. No claim is made that every query uses an index (three-row tables may be faster with sequential scans). Warm-cache lookup timings compare actual Redis retrieval/JSON decoding against SQL queries on one reused connection; the result is not a measured index speedup.

Metadata/batch endpoints are a single-tenant demonstration. Traces, services and alerts apply ownership filtering; batch-status lookup is not tenant-isolated, and rules are configured by the trusted SQL operator. The UI holds a token only in its input field. No public deployment, abuse prevention or enterprise security claims are made.

## Alert and webhook semantics

Each rule tests complete-window p99 upper bound `>` threshold and a minimum sample count. A breach starts PENDING; continuous evaluations for `for_seconds` promote FIRING. No data or recovery resolves an open episode. Gaps over 20 seconds reset pending continuity; repeated/backdated evaluation timestamps are ignored. A unique partial SQL index permits one open alert per rule. Creation of an alert and its webhook outbox record is transactional.

The sender claims an outbox row with a 30-second tokenized lease, sends HTTP in the background with a three-second timeout and event ID as Idempotency-Key, and records delivery with token fencing. Each send task drains up to 20 rows. Failed sends retry on later ticks (at most five attempts). After exhaustion, the current demo leaves a PENDING/SENDING row for operator inspection. The local receiver stores unique event IDs transactionally, making replay safe there. An external receiver must implement its own deduplication: webhook HTTP delivery is at least once, not universally exactly once.

Tests notify only the bundled local sink. No Slack messages or real emails are sent. Connectors to external channels are not installed or used. External channel delivery adapters are a future integration, not a claimed finished Slack/email feature.

## Retention and scaling limits

Admission payloads and dedup hashes expire after 24 hours. Retry/drain must complete inside that window; an expired payload fails rather than reconstructing old metrics. Completed outbox stream entries are retained and need monitored cleanup. Rolling buckets expire after 90 seconds; cumulative counters persist until Redis state is lost/reset. Sampled SQL traces and alerts need explicit retention policies. No automatic retention job or bounded total Redis/SQL backlog is implemented.

The exported runtime snapshot is the latest one agent received, not a per-instance fleet view. Prometheus exporter construction reads the registered service list and window hashes at scrape time. The API, relay and Celery workers can scale, but PostgreSQL, Redis and Prometheus are single instances in Compose. Redis Cluster, multi-host failover, bounded backpressure, large service-cardinality deployments, HA Prometheus, and disk-loss recovery were not tested.
