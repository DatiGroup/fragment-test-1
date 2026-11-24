# Test Fragment 1 — Counter Events Producer

Reference test fragment used as a learning example for the digital-twin
platform. The worker maintains an integer counter that it increments on a
fixed interval, persists the value through the API, publishes a counter
event to Kafka, and emits operational health events to the platform-wide
`monitoring.ingest-events` topic. The API exposes the counter over REST
and a WebSocket stream consumed by the `simple-number` widget. This
fragment is paired with test-fragment-2 (Kafka consumer) and
test-fragment-3 (REST poller) to demonstrate end-to-end data flow between
fragments.

**Repository:** https://github.com/DatiGroup/fragment-test-1
**Fragment ID:** `test-fragment-1`

## Architecture

The fragment is composed of a worker that drives the counter and an API
service that persists the value and serves it to widgets. The worker also
emits heartbeats to the `monitoring.ingest-events` topic for the
data-ingest-monitor.

```
   ┌──────────┐                                    ┌──────────────────┐
   │  Worker  │ ──▶ Kafka: f1.events ───────▶      │     consumers    │
   │          │ ──▶ Kafka: monitoring.* ─▶         │ (test-fragment-2,│
   │          │                                    │  data-ingest-monitor)
   │          │ ──▶ REST POST /counter ─▶ API ─▶ PostgreSQL
   └──────────┘                          │
                                         ├─▶ REST GET /counter
                                         └─▶ WebSocket /ws/counter
```

### Components

- **Worker** — increments the counter every `API_INTERVAL` seconds by
  reading the current value from the API, publishing a `CounterEvent` to
  Kafka, then posting `value+1` back, and sends a monitoring event after each
  attempt.
- **API** — FastAPI service backed by PostgreSQL. Exposes REST endpoints
  for reading and writing the counter and a WebSocket that streams the
  current value at a configurable cadence. The counter is a single row in the
  `counter` table, created on startup (Alembic migration, plus `create_all` as
  a fallback).

### One tick

1. `GET /counter` → `current`.
2. Publish `{event_id, fragment_id, counter: current+1, delta: 1, timestamp,
   metadata: {source: "worker"}}` to `KAFKA_TOPIC` (no message key). A failed
   publish is logged and does not stop the tick.
3. `POST /counter {"value": current+1}`.
4. On success, a monitoring `heartbeat` (`status: ok`, `records_processed: 1`)
   — or a `status_change` if the previous tick had failed.

A failure in steps 1–3 retries the whole tick up to `API_RETRIES` times,
`API_SLEEP_ON_ERROR` seconds apart; when all fail, one `status_change` with
`status: error` is sent and the worker waits another `API_SLEEP_ON_ERROR`
seconds. The worker then sleeps `API_INTERVAL` seconds, so the period is the
interval plus the time the tick took.

Because the Kafka event goes out **before** the API write, a tick whose
`POST /counter` fails and is retried publishes the same `counter` value again
under a new `event_id` — consumers can see an increment that never reached the
API, or the same value twice.

## REST API endpoints

| Method | Path           | Description                                  |
|--------|----------------|----------------------------------------------|
| GET    | `/healthcheck` | Health status                                |
| GET    | `/counter`     | Get current counter value, `{"value": n}`; `0` before the first write |
| POST   | `/counter`     | Set the counter value, `{"value": n}` (used by worker); any integer is accepted |

## WebSocket endpoints

| Path           | Description                                                            |
|----------------|------------------------------------------------------------------------|
| `/ws/counter`  | Streams the current counter value as `{value, timestamp}` on connect and then every `COUNTER_WS_INTERVAL` seconds, whether or not it changed. |

## Kafka topics

### Produces

| Topic                       | Payload class                            | Description                                              |
|-----------------------------|------------------------------------------|----------------------------------------------------------|
| `f1.events`                 | `com.example.f1.Event`                   | One event per increment: the new `counter` value and `delta` (always `1`). |
| `monitoring.ingest-events`  | `com.digitdevops.monitoring.IngestEvent` | Heartbeat and status events for `data-ingest-monitor`, one per tick. |

The fragment does not consume any Kafka topic. test-fragment-2 declares the
same `f1.events` payload as `com.example.f1.CounterEvent`; the schemas match,
the type names do not.

Kafka is optional. Without `KAFKA_BOOTSTRAP_SERVERS` the worker still runs
and increments the counter, but publishes nothing — **monitoring events
included**, so `data-ingest-monitor` never sees it. Monitoring events are also
skipped whenever the Kafka producer could not be created.

## Authentication

The API uses
[dt-fragment-auth](https://github.com/DatiGroup/dt-fragment-auth)
and refuses every request without a valid caller, including routes added
later. Accepted callers:

- **Portal widgets** send the signed-in user's Keycloak token
  (`Authorization: Bearer <token>`; on the WebSocket, a first
  message `{"type": "authenticate", "token": "<token>"}`, before which
  nothing is sent).
  Only members of this digital twin are admitted.
- **Other fragments of the same twin** send a token of the twin's Keycloak
  client.
- **This fragment's own worker** sends `X-Fragment-Token: <FRAGMENT_INTERNAL_TOKEN>`.

The platform injects everything below at deploy time; none of it goes in the
instance config. In `enforce` or `log` mode the API refuses to start when a
required variable is missing.

| Variable | Component | Description |
|---|---|---|
| `FRAGMENT_AUTH_MODE` | api | `enforce` (platform), `log` (check and log, refuse nothing) or `off` (the local Docker Compose stack) |
| `FRAGMENT_AUTH_JWKS_URL`, `FRAGMENT_AUTH_ISSUER`, `FRAGMENT_AUTH_AUDIENCE` | api | Keycloak token validation |
| `FRAGMENT_AUTH_DT_API_URL`, `DIGITAL_TWIN_ID` | api | Twin membership check for user tokens |
| `FRAGMENT_AUTH_TWIN_CLIENT_ID` | api | The twin's Keycloak client (`dt-<short-twin-id>`), accepted from same-twin fragments |
| `FRAGMENT_INTERNAL_TOKEN` | api, worker | This instance's internal token, from its Kubernetes Secret |

## Configuration

Variables read by the code. **None of them is declared in `manifest.yaml`**,
so none can be set per instance. `k8s/manifest.yaml` hard-codes `API_INTERVAL`, `API_RETRIES` and
`API_SLEEP_ON_ERROR` to the values below, and does not pass
`COUNTER_WS_INTERVAL` to the api at all.

| Variable                | Component | Description                                | Default            |
|-------------------------|-----------|--------------------------------------------|--------------------|
| `API_INTERVAL`          | worker    | Seconds between counter increments         | `30`               |
| `API_RETRIES`           | worker    | Attempts per tick                          | `3`                |
| `API_SLEEP_ON_ERROR`    | worker    | Seconds to wait after a failed attempt     | `5`                |
| `COUNTER_WS_INTERVAL`   | api       | WebSocket emit interval, in seconds        | `2`                |
| `KAFKA_BOOTSTRAP_SERVERS` | worker  | Kafka brokers; empty disables Kafka         | —                  |
| `KAFKA_TOPIC`           | worker    | Kafka topic for counter events             | `f1.events`        |
| `KAFKA_TOPIC_MONITORING`| worker    | Kafka topic for monitoring events          | `monitoring.ingest-events` |
| `API_URL`               | worker    | Fragment API base URL, set by the platform | `http://localhost:8000` |
| `API_COUNTER_GET_ENDPOINT`, `API_COUNTER_POST_ENDPOINT` | worker | Override the two counter URLs | `{API_URL}/counter` |
| `FRAGMENT_ID`, `FRAGMENT_NAME` | worker | `fragment_id` on counter events; `source` / `source_name` on monitoring events | `test-fragment-1`, — |
| `DB_NAME`, `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD` / `DATABASE_URL` | api | Database connection | port `5432` |

Both components also load a `.env` file if present; copy `.env.example`
to `.env` to start one.

## Sample configuration

What the digital-twin builder stores for this fragment. You enter it in the
builder, not in a file; the platform turns it into environment variables and
Kubernetes resources at deploy time.

### Instance config

This fragment has no operator settings, so an instance only needs config
to change its CPU or memory:

```json
{
  "resources": {
    "api":    { "memoryRequest": "128Mi", "memoryLimit": "512Mi" },
    "worker": { "memoryRequest": "128Mi", "memoryLimit": "512Mi" }
  }
}
```

`resources` is optional: it overrides the CPU and memory in
`k8s/manifest.yaml`, whose values the builder pre-fills (shown above). The keys
are `cpuRequest`, `cpuLimit`, `memoryRequest` and `memoryLimit`; flat keys apply
to both containers, an `api` or `worker` block to one. Leave the config empty
(`{}`) to keep the defaults.

### Connections

Drawing a connection in the builder stores a record like these (shown without
the fragment and widget ids).

Kafka, to test-fragment-2. `config.topicName` is the one topic both ends use;
the platform deploys it as `<short-twin-id>.f1.events`.

```json
{
  "type": "kafka",
  "from_config": { "topicKey": "f1.events", "topic": "f1.events" },
  "to_config":   { "topicKey": "f1.events", "topic": "f1.events" },
  "config":      { "topicName": "f1.events" }
}
```

WebSocket, to the simple-number widget's `counter-stream` slot:

```json
{
  "type": "websocket",
  "from_config": { "wsPath": "/ws/counter" },
  "to_config":   { "key": "counter-stream" }
}
```

REST, to the same widget slot, used when no WebSocket is wired (the widget
fetches once, or polls when its `pollInterval` is set; `GET /counter` returns
`{"value": <n>}`):

```json
{
  "type": "rest",
  "from_config": { "apiPath": "/counter", "method": "GET" },
  "to_config":   { "key": "counter-stream", "method": "GET" }
}
```

## Dependencies

- **Platform fragments:** none. (Used by `data-ingest-monitor` and consumed
  by test-fragment-2 in the canonical test-twin layout.)
- **External services:** none.

## Local development

### With Docker

```bash
docker compose up --build
```

- API: <http://localhost:8000> (OpenAPI docs at `/docs`)
- Kafka UI: <http://localhost:8080>
- Kafka broker from the host: `localhost:9092`

The worker increments the counter every 30 seconds by default. Drop
`API_INTERVAL` in `docker-compose.yml` to a small value (e.g. `2`) to watch
the WebSocket and Kafka topic update quickly.

### Without Docker (standalone, Windows/PowerShell)

`run-local.ps1` runs the api and worker directly from source — no Docker,
no orchestrator, no digital-twin instance. It creates an isolated `.venv`,
installs a Python 3.14-compatible dependency set (the pinned
`requirements.txt` predates 3.14), starts the FastAPI app against a local
**SQLite** database (zero external services), starts the worker with Kafka
disabled, waits for `/healthcheck`, then tails the worker log. Press
`Ctrl+C` to stop both. No tracked files are modified — the database and
worker settings are supplied purely via environment variables at launch.

```powershell
# api (SQLite) + worker, incrementing every 3s
.\run-local.ps1

# faster ticks and a custom API port
.\run-local.ps1 -WorkerInterval 1 -ApiPort 8001

# api only (skip the worker)
.\run-local.ps1 -NoWorker

# enable the Kafka publish path against a local broker
.\run-local.ps1 -KafkaBootstrap "localhost:9092"

# rebuild the virtual environment from scratch
.\run-local.ps1 -Recreate

# stop an instance started by a previous run
.\run-local.ps1 -Stop
```

- API: <http://127.0.0.1:8000>  ·  Docs: <http://127.0.0.1:8000/docs>
- Counter: <http://127.0.0.1:8000/counter>  ·  WebSocket: `ws://127.0.0.1:8000/ws/counter`
- Logs are written under `.run/`; the SQLite file is `api/counter.db`
  (both are git-ignored).

For real PostgreSQL parity instead of SQLite, pass a `DATABASE_URL`
(requires `psycopg2` in the venv and that the database/role exist):

```powershell
.\run-local.ps1 -DatabaseUrl "postgresql://fragment-1-user:<password>@localhost:5433/fragment-1"
```

## Schemas

- Kafka payload schemas: `schemas/produces/kafka/*.json`
- WebSocket payload schemas: `schemas/produces/websocket/*.json`
- REST API description: `schemas/produces/openapi/produces.openapi.json`

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
