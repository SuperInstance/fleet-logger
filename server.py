"""HTTP server for the Fleet Logger.

Exposes a REST API for log ingestion, querying, stats, tracing, and
real-time tail.  Uses only the Python stdlib (:mod:`http.server`).

Endpoints
---------
POST /ingest          — ingest one or many log entries (JSON body)
GET  /search           — query logs (query-string filters)
GET  /agents           — list agents with log counts
GET  /stats            — aggregate log statistics
GET  /trace/<trace_id> — all logs for a trace
GET  /tail             — long-poll for new entries
DELETE /prune          — prune old logs
GET  /health           — health check
"""

from __future__ import annotations

import json
import threading
import time
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any, Optional

from logger import FleetLogger, LogEntry, DEFAULT_LOG_DIR, DEFAULT_PORT
from query import LogQuery


# ---------------------------------------------------------------------------
# Request handler
# ---------------------------------------------------------------------------
class LoggerHandler(BaseHTTPRequestHandler):
    """Routes incoming HTTP requests to FleetLogger / LogQuery methods."""

    # Shared across all request instances (set once by serve())
    fleet_logger: FleetLogger  # type: ignore[assignment]
    log_query: LogQuery        # type: ignore[assignment]

    # Suppress default stderr logging
    def log_message(self, fmt: str, *args: Any) -> None:
        pass

    # -- Helpers -------------------------------------------------------------

    def _send_json(self, data: Any, status: int = 200) -> None:
        body = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, msg: str, status: int = 400) -> None:
        self._send_json({"error": msg}, status)

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length)

    def _parse_qs(self) -> dict[str, str]:
        qs = urllib.parse.parse_qs(self.path.split("?", 1)[-1], keep_blank_values=True)
        return {k: v[0] for k, v in qs.items()}

    def _route(self) -> tuple[str, dict[str, str]]:
        """Parse path into (route, params)."""
        path = self.path.split("?")[0]
        parts = [p for p in path.strip("/").split("/") if p]
        params = self._parse_qs()
        return "/".join(parts), params

    # -- GET -----------------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802
        route, params = self._route()

        if route == "health":
            self._handle_health()
        elif route == "search":
            self._handle_search(params)
        elif route == "agents":
            self._handle_agents()
        elif route == "stats":
            self._handle_stats()
        elif route == "tail":
            self._handle_tail(params)
        elif route.startswith("trace/"):
            trace_id = route.split("/", 1)[1]
            self._handle_trace(trace_id)
        else:
            self._send_error(f"Unknown route: GET /{route}", 404)

    # -- POST ----------------------------------------------------------------

    def do_POST(self) -> None:  # noqa: N802
        route, _ = self._route()
        if route == "ingest":
            self._handle_ingest()
        else:
            self._send_error(f"Unknown route: POST /{route}", 404)

    # -- DELETE --------------------------------------------------------------

    def do_DELETE(self) -> None:  # noqa: N802
        route, params = self._route()
        if route == "prune":
            self._handle_prune(params)
        else:
            self._send_error(f"Unknown route: DELETE /{route}", 404)

    # -- Endpoint implementations -------------------------------------------

    def _handle_health(self) -> None:
        stats = self.fleet_logger.stats()
        self._send_json({
            "status": "ok",
            "entries": stats["total_entries"],
            "agents": stats["agents_count"],
            "uptime_since": stats["earliest_ts"],
        })

    def _handle_ingest(self) -> None:
        try:
            body = json.loads(self._read_body())
        except (json.JSONDecodeError, Exception) as exc:
            self._send_error(f"Invalid JSON: {exc}")
            return

        entries: list[LogEntry] = []
        # Support both single entry and batch (list)
        data = body if isinstance(body, list) else [body]
        for item in data:
            try:
                entries.append(LogEntry.from_dict(item))
            except (TypeError, ValueError) as exc:
                self._send_error(f"Invalid entry: {exc}")
                return

        count = self.fleet_logger.ingest_batch(entries)
        self._send_json({"ingested": count, "status": "ok"})

    def _handle_search(self, params: dict[str, str]) -> None:
        result = self.log_query.search(
            agent=params.get("agent"),
            level=params.get("level"),
            min_ts=self._float(params.get("min_ts")),
            max_ts=self._float(params.get("max_ts")),
            trace_id=params.get("trace_id"),
            session_id=params.get("session_id"),
            pattern=params.get("pattern"),
            sort_desc=params.get("sort", "desc").lower() == "desc",
            limit=int(params.get("limit", "100")),
            offset=int(params.get("offset", "0")),
        )
        self._send_json(result)

    def _handle_agents(self) -> None:
        agents = self.fleet_logger.list_agents()
        self._send_json({"agents": agents, "count": len(agents)})

    def _handle_stats(self) -> None:
        self._send_json(self.fleet_logger.stats())

    def _handle_trace(self, trace_id: str) -> None:
        result = self.log_query.trace(trace_id)
        self._send_json(result)

    def _handle_tail(self, params: dict[str, str]) -> None:
        timeout = float(params.get("timeout", "30"))
        entries = self.log_query.tail(
            agent=params.get("agent"),
            level=params.get("level"),
            timeout=timeout,
        )
        self._send_json({
            "entries": [e.to_dict() for e in entries],
            "count": len(entries),
        })

    def _handle_prune(self, params: dict[str, str]) -> None:
        days = int(params.get("days", "30"))
        removed = self.fleet_logger.prune(days)
        self._send_json({"pruned": removed, "retention_days": days})

    @staticmethod
    def _float(value: Optional[str]) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except ValueError:
            return None


# ---------------------------------------------------------------------------
# Server bootstrap
# ---------------------------------------------------------------------------
def serve(
    host: str = "0.0.0.0",
    port: int = DEFAULT_PORT,
    log_dir: str = DEFAULT_LOG_DIR,
    retention_days: int = 30,
) -> None:
    """Start the Fleet Logger HTTP server (blocking)."""
    fleet_logger = FleetLogger(
        log_dir=log_dir,
        retention_days=retention_days,
    )
    fleet_logger.rebuild_index()

    log_query = LogQuery(fleet_logger)

    # Inject shared state into handler class
    LoggerHandler.fleet_logger = fleet_logger
    LoggerHandler.log_query = log_query

    server = HTTPServer((host, port), LoggerHandler)

    print(f"[fleet-logger] Listening on {host}:{port}")
    print(f"[fleet-logger] Log directory: {log_dir}")
    print(f"[fleet-logger] Retention: {retention_days} days")
    print(f"[fleet-logger] Loaded {len(fleet_logger._index)} entries from disk")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[fleet-logger] Shutting down…")
    finally:
        fleet_logger.close()
        server.server_close()
        print("[fleet-logger] Server stopped.")
