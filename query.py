"""Query engine for fleet logs.

Provides a high-level API for searching, filtering, aggregating, and
exporting log entries stored by :class:`FleetLogger`.

Supports:
  - Multi-criteria filtering (agent, level, time range, trace_id, regex)
  - Sort by timestamp (ascending / descending)
  - Pagination via limit + offset
  - Aggregation (count, group_by agent/level/hour)
  - Real-time tail via callback
  - Export to JSON, plain text, or CSV
"""

from __future__ import annotations

import csv
import io
import json
import re
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Callable, Iterator, Optional

from logger import FleetLogger, LogEntry


class LogQuery:
    """Query language / engine for fleet logs.

    Instances are lightweight and stateless — they delegate all storage
    operations to a :class:`FleetLogger` reference.
    """

    def __init__(self, fleet_logger: FleetLogger) -> None:
        self._logger = fleet_logger
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Core search
    # ------------------------------------------------------------------

    def search(
        self,
        *,
        agent: Optional[str] = None,
        level: Optional[str] = None,
        min_ts: Optional[float] = None,
        max_ts: Optional[float] = None,
        trace_id: Optional[str] = None,
        session_id: Optional[str] = None,
        pattern: Optional[str] = None,
        sort_desc: bool = True,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        """Execute a search and return a result dict with metadata."""
        entries = self._logger.search(
            agent=agent,
            level=level,
            min_ts=min_ts,
            max_ts=max_ts,
            trace_id=trace_id,
            session_id=session_id,
            pattern=pattern,
            sort_desc=sort_desc,
            limit=limit,
            offset=offset,
        )
        total_matched = len(entries)
        return {
            "results": [e.to_dict() for e in entries],
            "count": total_matched,
            "limit": limit,
            "offset": offset,
        }

    def search_simple(self, query: str, *, limit: int = 50) -> dict[str, Any]:
        """Parse a simple query string and search.

        Supported syntax:
          ``level:ERROR``
          ``agent:orchestrator``
          ``trace:abc123``
          ``after:2025-01-01T00:00:00``
          ``before:2025-12-31T23:59:59``
          Free-form text is treated as a message regex.
        """
        agent = None
        level = None
        trace_id = None
        min_ts: Optional[float] = None
        max_ts: Optional[float] = None
        pattern = None

        tokens = query.split()
        remaining: list[str] = []
        for token in tokens:
            if token.startswith("level:"):
                level = token.split(":", 1)[1]
            elif token.startswith("agent:"):
                agent = token.split(":", 1)[1]
            elif token.startswith("trace:"):
                trace_id = token.split(":", 1)[1]
            elif token.startswith("after:"):
                min_ts = self._parse_ts(token.split(":", 1)[1])
            elif token.startswith("before:"):
                max_ts = self._parse_ts(token.split(":", 1)[1]
)
            else:
                remaining.append(token)

        if remaining:
            pattern = " ".join(remaining)

        return self.search(
            agent=agent,
            level=level,
            min_ts=min_ts,
            max_ts=max_ts,
            trace_id=trace_id,
            pattern=pattern,
            limit=limit,
        )

    # ------------------------------------------------------------------
    # Trace
    # ------------------------------------------------------------------

    def trace(self, trace_id: str) -> dict[str, Any]:
        """Fetch all entries for a trace, sorted chronologically."""
        entries = self._logger.trace(trace_id)
        return {
            "trace_id": trace_id,
            "entries": [e.to_dict() for e in entries],
            "count": len(entries),
        }

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    def aggregate(
        self,
        *,
        group_by: str = "agent",
        min_ts: Optional[float] = None,
        max_ts: Optional[float] = None,
    ) -> dict[str, Any]:
        """Aggregate log counts by *group_by* dimension.

        *group_by* can be ``agent``, ``level``, or ``hour``.
        """
        entries = self._logger.search(
            min_ts=min_ts,
            max_ts=max_ts,
            limit=1_000_000,
            sort_desc=False,
        )

        groups: dict[str, int] = defaultdict(int)
        for entry in entries:
            if group_by == "agent":
                key = entry.agent
            elif group_by == "level":
                key = entry.level
            elif group_by == "hour":
                ts = datetime.fromtimestamp(entry.timestamp, tz=timezone.utc)
                key = ts.strftime("%Y-%m-%dT%H:00")
            else:
                raise ValueError(f"Unknown group_by: {group_by!r}")
            groups[key] += 1

        sorted_groups = sorted(groups.items(), key=lambda x: -x[1])
        return {
            "group_by": group_by,
            "groups": sorted_groups,
            "total": len(entries),
        }

    # ------------------------------------------------------------------
    # Real-time tail
    # ------------------------------------------------------------------

    def tail(
        self,
        *,
        agent: Optional[str] = None,
        level: Optional[str] = None,
        timeout: float = 30.0,
        max_entries: int = 0,
    ) -> list[LogEntry]:
        """Block until new log entries arrive or *timeout* elapses.

        Returns up to *max_entries* new entries (0 = unlimited).
        Applies *agent* and *level* filters on the fly.
        """
        collected: list[LogEntry] = []
        done = threading.Event()
        min_sev = 0
        if level:
            from logger import LEVEL_ORDER
            min_sev = LEVEL_ORDER.get(level.upper(), 0)

        def _callback(entry: LogEntry) -> None:
            if agent and entry.agent != agent:
                return
            if entry.level in getattr(self._logger, "__dict__", {}):
                pass
            from logger import LEVEL_ORDER
            if LEVEL_ORDER.get(entry.level, 0) < min_sev:
                return
            collected.append(entry)
            if max_entries > 0 and len(collected) >= max_entries:
                done.set()

        sub_id = self._logger.subscribe_tail(_callback)
        try:
            done.wait(timeout=timeout)
        finally:
            self._logger.unsubscribe_tail(sub_id)

        return collected

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def export(
        self,
        *,
        fmt: str = "json",
        agent: Optional[str] = None,
        level: Optional[str] = None,
        min_ts: Optional[float] = None,
        max_ts: Optional[float] = None,
        pattern: Optional[str] = None,
        limit: int = 10_000,
    ) -> str:
        """Export matching logs in the requested format.

        Supported formats: ``json``, ``text``, ``csv``.
        """
        entries = self._logger.search(
            agent=agent,
            level=level,
            min_ts=min_ts,
            max_ts=max_ts,
            pattern=pattern,
            limit=limit,
        )

        if fmt == "json":
            return json.dumps(
                [e.to_dict() for e in entries],
                indent=2,
                ensure_ascii=False,
            )
        elif fmt == "text":
            lines: list[str] = []
            for e in entries:
                ts = datetime.fromtimestamp(e.timestamp, tz=timezone.utc).isoformat()
                ctx = json.dumps(e.context) if e.context else ""
                line = f"[{ts}] [{e.level:8s}] {e.agent:20s} {e.message}"
                if e.trace_id:
                    line += f"  trace={e.trace_id}"
                if ctx:
                    line += f"  {ctx}"
                lines.append(line)
            return "\n".join(lines)
        elif fmt == "csv":
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(["timestamp", "agent", "level", "message", "trace_id", "session_id", "context"])
            for e in entries:
                ts = datetime.fromtimestamp(e.timestamp, tz=timezone.utc).isoformat()
                writer.writerow([
                    ts, e.agent, e.level, e.message,
                    e.trace_id, e.session_id, json.dumps(e.context),
                ])
            return buf.getvalue()
        else:
            raise ValueError(f"Unknown export format: {fmt!r}")

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_ts(s: str) -> Optional[float]:
        """Parse an ISO-8601 timestamp string to epoch float."""
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except (ValueError, TypeError):
            return None
