"""Centralized structured logging for the Pelagic fleet.

Agents send log entries via HTTP. Logger stores, indexes, and serves them.
Uses only Python stdlib — no external dependencies.

Log entries are stored as JSONL (JSON Lines) files, one per agent.
An in-memory index enables fast querying without loading entire files.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Optional


# ---------------------------------------------------------------------------
# Log levels — ordered by severity
# ---------------------------------------------------------------------------
LEVEL_ORDER = {"DEBUG": 0, "INFO": 1, "WARN": 2, "ERROR": 3, "CRITICAL": 4}
VALID_LEVELS = set(LEVEL_ORDER)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
@dataclass
class LogEntry:
    """A single structured log entry produced by a fleet agent."""

    timestamp: float
    agent: str
    level: str
    message: str
    context: dict = field(default_factory=dict)
    trace_id: str = ""
    session_id: str = ""

    def __post_init__(self) -> None:
        self.level = self.level.upper()
        if self.level not in VALID_LEVELS:
            raise ValueError(f"Invalid log level: {self.level!r}")

    # -- Serialisation helpers ------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LogEntry:
        return cls(**data)

    @classmethod
    def from_json(cls, raw: str) -> LogEntry:
        return cls.from_dict(json.loads(raw))


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DEFAULT_LOG_DIR = os.path.expanduser("~/.superinstance/logs/fleet")
DEFAULT_MAX_FILE_BYTES = 50 * 1024 * 1024  # 50 MiB per file
DEFAULT_RETENTION_DAYS = 30
DEFAULT_PORT = 8140


# ---------------------------------------------------------------------------
# FleetLogger — the central logging engine
# ---------------------------------------------------------------------------
class FleetLogger:
    """Centralized structured logging for the Pelagic fleet.

    Manages JSONL append-only storage, in-memory indexing, log rotation,
    retention-based pruning, and real-time tail subscriptions.

    Thread-safe: all public methods are protected by an internal lock.
    """

    def __init__(
        self,
        log_dir: str = DEFAULT_LOG_DIR,
        max_file_bytes: int = DEFAULT_MAX_FILE_BYTES,
        retention_days: int = DEFAULT_RETENTION_DAYS,
    ) -> None:
        self.log_dir = Path(log_dir)
        self.max_file_bytes = max_file_bytes
        self.retention_days = retention_days

        # In-memory index: list of lightweight record dicts
        self._index: list[dict[str, Any]] = []
        # Mapping agent -> list of index positions (for fast agent filtering)
        self._agent_map: dict[str, list[int]] = defaultdict(list)

        # Real-time tail subscribers: callbacks keyed by subscription id
        self._tail_subs: dict[str, Callable[[LogEntry], None]] = {}
        self._tail_lock = threading.Lock()

        self._lock = threading.RLock()
        self._file_handles: dict[str, Any] = {}  # agent -> open file handle
        self._file_sizes: dict[str, int] = defaultdict(int)

        self.log_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Ingestion
    # ------------------------------------------------------------------

    def ingest(self, entry: LogEntry) -> None:
        """Persist a single log entry and update the index."""
        with self._lock:
            self._write_entry(entry)
            self._add_to_index(entry)
            self._notify_tail(entry)

    def ingest_batch(self, entries: list[LogEntry]) -> int:
        """Persist a batch of log entries. Returns the count ingested."""
        with self._lock:
            for entry in entries:
                self._write_entry(entry)
                self._add_to_index(entry)
                self._notify_tail(entry)
            return len(entries)

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def get_entry(self, idx: int) -> Optional[LogEntry]:
        """Retrieve a log entry by its index position."""
        with self._lock:
            if 0 <= idx < len(self._index):
                rec = self._index[idx]
                return LogEntry.from_dict(rec)
            return None

    def all_entries(self) -> list[LogEntry]:
        """Return **all** indexed log entries (snapshot)."""
        with self._lock:
            return [LogEntry.from_dict(rec) for rec in self._index]

    def search(
        self,
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
    ) -> list[LogEntry]:
        """Search logs with optional filters.

        Parameters
        ----------
        agent : filter by agent name (exact match or glob * wildcard)
        level : minimum severity level
        min_ts / max_ts : timestamp bounds
        trace_id : filter by trace ID
        session_id : filter by session ID
        pattern : regex applied to the ``message`` field
        sort_desc : newest-first when True
        limit / offset : pagination
        """
        with self._lock:
            indices = self._resolve_indices(agent)
            regex = re.compile(pattern) if pattern else None
            min_sev = LEVEL_ORDER.get(level.upper(), 0) if level else 0

            results: list[LogEntry] = []
            for i in indices:
                rec = self._index[i]
                # Level filter
                if LEVEL_ORDER.get(rec["level"], 0) < min_sev:
                    continue
                # Timestamp bounds
                if min_ts is not None and rec["timestamp"] < min_ts:
                    continue
                if max_ts is not None and rec["timestamp"] > max_ts:
                    continue
                if trace_id and rec.get("trace_id") != trace_id:
                    continue
                if session_id and rec.get("session_id") != session_id:
                    continue
                if regex and not regex.search(rec["message"]):
                    continue
                results.append(LogEntry.from_dict(rec))

            results.sort(key=lambda e: e.timestamp, reverse=sort_desc)
            return results[offset : offset + limit]

    def trace(self, trace_id: str) -> list[LogEntry]:
        """Return all log entries for a given trace ID, sorted chronologically."""
        return self.search(trace_id=trace_id, sort_desc=False, limit=10_000)

    # ------------------------------------------------------------------
    # Agents & Stats
    # ------------------------------------------------------------------

    def list_agents(self) -> dict[str, int]:
        """Return mapping of agent name -> log count."""
        with self._lock:
            return {
                agent: len(positions) for agent, positions in self._agent_map.items()
            }

    def stats(self) -> dict[str, Any]:
        """Aggregate statistics across all logs."""
        with self._lock:
            total = len(self._index)
            level_counts: dict[str, int] = defaultdict(int)
            agent_counts: dict[str, int] = defaultdict(int)
            hourly_counts: dict[str, int] = defaultdict(int)

            for rec in self._index:
                level_counts[rec["level"]] += 1
                agent_counts[rec["agent"]] += 1
                ts = datetime.fromtimestamp(rec["timestamp"], tz=timezone.utc)
                hour_key = ts.strftime("%Y-%m-%dT%H:00")
                hourly_counts[hour_key] += 1

            # Top 5 agents by log volume
            top_agents = sorted(agent_counts.items(), key=lambda x: -x[1])[:5]

            # Earliest / latest timestamp
            earliest = self._index[0]["timestamp"] if self._index else None
            latest = self._index[-1]["timestamp"] if self._index else None

            return {
                "total_entries": total,
                "agents_count": len(agent_counts),
                "level_counts": dict(level_counts),
                "agent_counts": dict(agent_counts),
                "top_agents": top_agents,
                "hourly_counts": dict(hourly_counts),
                "earliest_ts": earliest,
                "latest_ts": latest,
                "index_size": len(self._index),
                "storage_dir": str(self.log_dir),
            }

    # ------------------------------------------------------------------
    # Retention & Rotation
    # ------------------------------------------------------------------

    def prune(self, days: Optional[int] = None) -> int:
        """Delete log entries older than *days*. Returns entries removed."""
        days = days if days is not None else self.retention_days
        cutoff = time.time() - (days * 86400)
        removed = 0
        with self._lock:
            new_index: list[dict[str, Any]] = []
            new_agent_map: dict[str, list[int]] = defaultdict(list)
            for i, rec in enumerate(self._index):
                if rec["timestamp"] < cutoff:
                    removed += 1
                else:
                    new_pos = len(new_index)
                    new_index.append(rec)
                    new_agent_map[rec["agent"]].append(new_pos)
            self._index = new_index
            self._agent_map = new_agent_map
            # Rewrite JSONL files from index
            self._rewrite_files()
        return removed

    def rotate(self) -> int:
        """Rotate oversized log files. Returns number of files rotated."""
        rotated = 0
        with self._lock:
            for agent in list(self._agent_map.keys()):
                path = self._current_file_path(agent)
                if path.exists() and path.stat().st_size > self.max_file_bytes:
                    # Close existing handle
                    self._close_handle(agent)
                    # Rename to .rotated
                    rotated_path = self._rotated_path(agent)
                    path.rename(rotated_path)
                    rotated += 1
        return rotated

    # ------------------------------------------------------------------
    # Tail (real-time)
    # ------------------------------------------------------------------

    def subscribe_tail(self, callback: Callable[[LogEntry], None]) -> str:
        """Subscribe to real-time log entries. Returns subscription ID."""
        sub_id = uuid.uuid4().hex[:12]
        with self._tail_lock:
            self._tail_subs[sub_id] = callback
        return sub_id

    def unsubscribe_tail(self, sub_id: str) -> None:
        with self._tail_lock:
            self._tail_subs.pop(sub_id, None)

    def _notify_tail(self, entry: LogEntry) -> None:
        with self._tail_lock:
            for cb in self._tail_subs.values():
                try:
                    cb(entry)
                except Exception:
                    pass  # best-effort delivery

    # ------------------------------------------------------------------
    # Rebuild index from disk (used on startup)
    # ------------------------------------------------------------------

    def rebuild_index(self) -> int:
        """Scan JSONL files and rebuild the in-memory index.
        Returns the total number of entries loaded.
        """
        with self._lock:
            self._index.clear()
            self._agent_map.clear()
            self._file_sizes.clear()

            if not self.log_dir.exists():
                return 0

            all_entries: list[dict[str, Any]] = []
            for jsonl_file in sorted(self.log_dir.glob("*.jsonl")):
                agent_name = jsonl_file.stem
                with open(jsonl_file, "r", encoding="utf-8") as fh:
                    for line_no, line in enumerate(fh, 1):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                            all_entries.append(rec)
                        except json.JSONDecodeError:
                            # Skip malformed lines
                            continue

            # Sort by timestamp to maintain order
            all_entries.sort(key=lambda r: r.get("timestamp", 0))
            self._index = all_entries

            for i, rec in enumerate(self._index):
                self._agent_map[rec.get("agent", "unknown")].append(i)

            return len(self._index)

    # ------------------------------------------------------------------
    # Internal: writing & indexing
    # ------------------------------------------------------------------

    def _current_file_path(self, agent: str) -> Path:
        return self.log_dir / f"{agent}.jsonl"

    def _rotated_path(self, agent: str) -> Path:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        return self.log_dir / f"{agent}.{ts}.jsonl"

    def _get_handle(self, agent: str):
        if agent in self._file_handles:
            return self._file_handles[agent]
        path = self._current_file_path(agent)
        fh = open(path, "a", encoding="utf-8", buffering=1)  # line-buffered
        self._file_handles[agent] = fh
        if path.exists():
            self._file_sizes[agent] = path.stat().st_size
        return fh

    def _close_handle(self, agent: str) -> None:
        fh = self._file_handles.pop(agent, None)
        if fh:
            fh.close()

    def _write_entry(self, entry: LogEntry) -> None:
        fh = self._get_handle(entry.agent)
        line = entry.to_json() + "\n"
        fh.write(line)
        self._file_sizes[entry.agent] += len(line.encode("utf-8"))

        # Auto-rotate if oversized
        if self._file_sizes[entry.agent] > self.max_file_bytes:
            self._close_handle(entry.agent)
            old_path = self._current_file_path(entry.agent)
            if old_path.exists():
                old_path.rename(self._rotated_path(entry.agent))
            self._file_sizes[entry.agent] = 0

    def _add_to_index(self, entry: LogEntry) -> None:
        pos = len(self._index)
        self._index.append(entry.to_dict())
        self._agent_map[entry.agent].append(pos)

    def _resolve_indices(self, agent: Optional[str]) -> list[int]:
        if agent is None:
            return list(range(len(self._index)))
        if "*" in agent:
            # Simple glob: only * is supported as wildcard
            regex = re.compile("^" + agent.replace("*", ".*") + "$")
            indices: list[int] = []
            for a, positions in self._agent_map.items():
                if regex.match(a):
                    indices.extend(positions)
            return sorted(indices)
        return self._agent_map.get(agent, [])

    def _rewrite_files(self) -> None:
        """Rewrite all JSONL files from the current index."""
        # Close all handles
        for agent in list(self._file_handles.keys()):
            self._close_handle(agent)
        self._file_sizes.clear()

        # Group index records by agent
        agent_entries: dict[str, list[dict]] = defaultdict(list)
        for rec in self._index:
            agent_entries[rec["agent"]].append(rec)

        for agent, entries in agent_entries.items():
            path = self._current_file_path(agent)
            with open(path, "w", encoding="utf-8") as fh:
                for rec in entries:
                    fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
                self._file_sizes[agent] = fh.tell() if hasattr(fh, "tell") else 0

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def close(self) -> None:
        with self._lock:
            for agent in list(self._file_handles.keys()):
                self._close_handle(agent)
            self._tail_subs.clear()
