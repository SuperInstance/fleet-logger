# Fleet Logger — Centralized Structured Logging for the Pelagic Fleet

> ELK-lite for the fleet. Collect, index, query, and tail logs from all agents — using only the Python stdlib.

## Overview

Fleet Logger is a lightweight, centralized structured logging system designed for the Pelagic fleet of agents. It provides HTTP-based log ingestion, powerful search/query capabilities, real-time tailing, and automatic retention management — all without any external dependencies.

### Architecture

```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  Agent A     │  │  Agent B     │  │  Agent C     │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       │  POST /ingest   │  POST /ingest   │
       └────────────────┬─┴────────────────┘
                        ▼
              ┌─────────────────┐
              │   Fleet Logger   │
              │   HTTP Server    │
              │   (port 8140)    │
              └────────┬────────┘
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
    ┌──────────┐ ┌──────────┐ ┌──────────┐
    │  JSONL   │ │  In-Mem  │ │  Query   │
    │  Storage │ │  Index   │ │  Engine  │
    │ (per-    │ │ (ts,agent│ │ (filter, │
    │  agent)  │ │  ,level, │ │  sort,   │
    │          │ │  trace)  │ │  paginate│
    └──────────┘ └──────────┘ └──────────┘
```

## Log Entry Schema

Every log entry follows a structured format:

```json
{
  "timestamp": 1735689600.123,
  "agent": "orchestrator",
  "level": "INFO",
  "message": "Task completed successfully",
  "context": {"task_id": "t-42", "duration_ms": 234},
  "trace_id": "req-abc-123",
  "session_id": "sess-xyz-789"
}
```

| Field        | Type   | Description                                   |
|------------- | ------ | --------------------------------------------- |
| `timestamp`  | float  | Unix epoch (seconds, float for sub-ms precision) |
| `agent`      | string | Agent name that produced the log               |
| `level`      | string | `DEBUG`, `INFO`, `WARN`, `ERROR`, `CRITICAL`  |
| `message`    | string | Human-readable log message                     |
| `context`    | object | Optional structured data (key-value pairs)     |
| `trace_id`   | string | Correlates log entries across agents           |
| `session_id` | string | Groups entries by session                      |

## Quick Start

### Onboard

```bash
python cli.py onboard
```

Creates the log directory at `~/.superinstance/logs/fleet/` with a default config.

### Start the Server

```bash
python cli.py serve --port 8140
```

### Ingest Logs

```bash
curl -X POST http://localhost:8140/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "timestamp": 1735689600.0,
    "agent": "my-agent",
    "level": "INFO",
    "message": "Hello from the fleet",
    "trace_id": "trace-001"
  }'
```

### Search Logs

```bash
# Simple text search
python cli.py search "connection refused"

# Filter by level and agent
python cli.py search "level:ERROR agent:orchestrator"

# Via HTTP
curl "http://localhost:8140/search?level=ERROR&limit=50"
```

### Tail Logs (Real-time)

```bash
python cli.py tail --agent orchestrator --level WARN

# Or via HTTP long-poll
curl "http://localhost:8140/tail?timeout=30"
```

### Statistics

```bash
python cli.py stats
# or
curl http://localhost:8140/stats
```

## HTTP API

| Method   | Endpoint              | Description                              |
| -------- | --------------------- | ---------------------------------------- |
| `POST`   | `/ingest`             | Ingest one or many log entries (JSON)    |
| `GET`    | `/search`             | Query logs with filter parameters        |
| `GET`    | `/agents`             | List all agents with log counts          |
| `GET`    | `/stats`              | Aggregate log statistics                 |
| `GET`    | `/trace/{trace_id}`   | Get all logs for a trace ID              |
| `GET`    | `/tail`               | Long-poll for new log entries            |
| `DELETE` | `/prune?days=N`       | Delete logs older than N days            |
| `GET`    | `/health`             | Health check                             |

### Search Parameters

| Parameter    | Type   | Description                            |
| ------------ | ------ | -------------------------------------- |
| `agent`      | string | Filter by agent (supports `*` glob)    |
| `level`      | string | Minimum severity level                 |
| `min_ts`     | float  | Minimum timestamp (epoch)              |
| `max_ts`     | float  | Maximum timestamp (epoch)              |
| `trace_id`   | string | Filter by trace ID                     |
| `session_id` | string | Filter by session ID                   |
| `pattern`    | string | Regex to match against message         |
| `sort`       | string | `asc` or `desc` (default: `desc`)      |
| `limit`      | int    | Max results to return (default: 100)   |
| `offset`     | int    | Pagination offset (default: 0)         |

## CLI Reference

```
python cli.py serve [--port PORT] [--log-dir DIR] [--retention DAYS]
python cli.py search "<query>" [--agent NAME] [--level LEVEL] [--limit N]
python cli.py tail [--agent NAME] [--level LEVEL] [--timeout SEC]
python cli.py stats
python cli.py trace <trace_id>
python cli.py export [--format json|csv|text] [--output FILE]
python cli.py prune --days N
python cli.py onboard
```

### Query Syntax

The search query string supports a simple DSL:

| Token            | Example                          | Description                     |
| ---------------- | -------------------------------- | ------------------------------- |
| `level:X`        | `level:ERROR`                    | Filter by minimum level         |
| `agent:X`        | `agent:orchestrator`             | Filter by agent name            |
| `trace:X`        | `trace:abc-123`                  | Filter by trace ID              |
| `after:X`        | `after:2025-01-01T00:00:00`      | Filter entries after timestamp  |
| `before:X`       | `before:2025-12-31T23:59:59`     | Filter entries before timestamp |
| free text        | `connection refused`             | Regex match on message field    |

Tokens can be combined: `level:ERROR agent:svc-* database timeout`

## Storage

Logs are stored as **JSONL** (JSON Lines) files, one per agent:

```
~/.superinstance/logs/fleet/
├── orchestrator.jsonl
├── coder.jsonl
├── researcher.jsonl.20250101T120000.jsonl   # rotated file
└── config.json
```

- **Append-only**: new entries are always appended
- **Auto-rotation**: files are rotated when they exceed 50 MiB
- **Retention**: entries older than 30 days are pruned by default
- **In-memory index**: fast querying without full file scans

## Configuration

Default config is created during `onboard`:

```json
{
  "log_dir": "~/.superinstance/logs/fleet",
  "retention_days": 30,
  "max_file_bytes": 52428800,
  "created_at": "2025-01-01T00:00:00+00:00"
}
```

## Running Tests

```bash
cd fleet/fleet-logger
python -m pytest tests/test_fleet_logger.py -v
# or
python -m unittest tests.test_fleet_logger -v
```

## Design Principles

- **Stdlib only**: zero external dependencies — runs anywhere Python 3.10+ is available
- **Append-only storage**: JSONL files are never mutated in place (except during prune)
- **In-memory index**: fast lookups without full file scans
- **Thread-safe**: all public methods are lock-protected for concurrent access
- **Graceful degradation**: corrupted JSONL lines are skipped, not fatal
- **Simple query DSL**: no need to learn a full query language

## License

MIT
