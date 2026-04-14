"""Tests for the Fleet Logger — centralized structured logging.

Covers logger core, query engine, and HTTP server using only stdlib
(:mod:`unittest` + :mod:`http.client`).

Run with:
    python -m pytest tests/test_fleet_logger.py -v
    # or
    python -m unittest tests.test_fleet_logger -v
"""

from __future__ import annotations

import io
import json
import os
import shutil
import tempfile
import threading
import time
import unittest
import urllib.request
import urllib.error
from http.client import HTTPConnection
from pathlib import Path
from typing import Any

# Ensure the fleet-logger package is importable
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from logger import (
    DEFAULT_LOG_DIR,
    FleetLogger,
    LogEntry,
    LEVEL_ORDER,
    VALID_LEVELS,
)
from query import LogQuery
from server import serve, LoggerHandler


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _tmp_log_dir() -> str:
    """Return a path to a temporary log directory that is cleaned up later."""
    d = tempfile.mkdtemp(prefix="fleet-logger-test-")
    return d


def _make_entry(
    agent: str = "test-agent",
    level: str = "INFO",
    message: str = "hello world",
    trace_id: str = "",
    session_id: str = "",
    context: dict | None = None,
    timestamp: float | None = None,
) -> LogEntry:
    return LogEntry(
        timestamp=timestamp or time.time(),
        agent=agent,
        level=level,
        message=message,
        trace_id=trace_id,
        session_id=session_id,
        context=context or {},
    )


def _make_entries(n: int, agent: str = "test-agent") -> list[LogEntry]:
    """Generate *n* log entries with incrementing timestamps."""
    base = time.time() - n
    entries: list[LogEntry] = []
    for i in range(n):
        entries.append(_make_entry(
            agent=agent,
            level=["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"][i % 5],
            message=f"log message {i}",
            timestamp=base + i,
        ))
    return entries


# ---------------------------------------------------------------------------
# Test: LogEntry data model
# ---------------------------------------------------------------------------

class TestLogEntry(unittest.TestCase):
    """Unit tests for the LogEntry dataclass."""

    def test_valid_levels(self) -> None:
        for level in VALID_LEVELS:
            entry = _make_entry(level=level.lower())  # should be uppercased
            self.assertEqual(entry.level, level)

    def test_invalid_level_raises(self) -> None:
        with self.assertRaises(ValueError):
            _make_entry(level="FOOBAR")

    def test_round_trip_json(self) -> None:
        entry = _make_entry(trace_id="abc", session_id="sess1", context={"key": "val"})
        serialized = entry.to_json()
        restored = LogEntry.from_json(serialized)
        self.assertEqual(restored.agent, entry.agent)
        self.assertEqual(restored.level, entry.level)
        self.assertEqual(restored.trace_id, entry.trace_id)
        self.assertEqual(restored.context, entry.context)

    def test_round_trip_dict(self) -> None:
        entry = _make_entry()
        d = entry.to_dict()
        restored = LogEntry.from_dict(d)
        self.assertEqual(restored.message, entry.message)


# ---------------------------------------------------------------------------
# Test: FleetLogger core
# ---------------------------------------------------------------------------

class TestFleetLogger(unittest.TestCase):
    """Tests for the central FleetLogger engine."""

    def setUp(self) -> None:
        self.log_dir = _tmp_log_dir()
        self.logger = FleetLogger(log_dir=self.log_dir, retention_days=90)

    def tearDown(self) -> None:
        self.logger.close()
        shutil.rmtree(self.log_dir, ignore_errors=True)

    def test_ingest_single(self) -> None:
        entry = _make_entry(agent="alpha")
        self.logger.ingest(entry)
        agents = self.logger.list_agents()
        self.assertIn("alpha", agents)
        self.assertEqual(agents["alpha"], 1)

    def test_ingest_batch(self) -> None:
        entries = _make_entries(10, agent="beta")
        count = self.logger.ingest_batch(entries)
        self.assertEqual(count, 10)
        agents = self.logger.list_agents()
        self.assertEqual(agents["beta"], 10)

    def test_search_by_agent(self) -> None:
        self.logger.ingest(_make_entry(agent="alpha"))
        self.logger.ingest(_make_entry(agent="beta"))
        self.logger.ingest(_make_entry(agent="alpha"))

        results = self.logger.search(agent="alpha")
        self.assertEqual(len(results), 2)
        for r in results:
            self.assertEqual(r.agent, "alpha")

    def test_search_by_level(self) -> None:
        self.logger.ingest(_make_entry(level="DEBUG"))
        self.logger.ingest(_make_entry(level="INFO"))
        self.logger.ingest(_make_entry(level="ERROR"))

        # search(level="WARN") sets min_sev=2 (WARN).
        # Only entries with severity >= 2 match: WARN, ERROR, CRITICAL.
        # We only ingested ERROR (3), so we expect 1 result.
        results = self.logger.search(level="WARN")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].level, "ERROR")

    def test_search_by_trace_id(self) -> None:
        trace = "trace-123"
        self.logger.ingest(_make_entry(trace_id=trace, message="step 1"))
        self.logger.ingest(_make_entry(trace_id="other", message="unrelated"))
        self.logger.ingest(_make_entry(trace_id=trace, message="step 2"))

        results = self.logger.search(trace_id=trace)
        self.assertEqual(len(results), 2)

    def test_search_by_pattern(self) -> None:
        self.logger.ingest(_make_entry(message="database connection failed"))
        self.logger.ingest(_make_entry(message="cache hit"))
        self.logger.ingest(_make_entry(message="database query timeout"))

        results = self.logger.search(pattern=r"database.*")
        self.assertEqual(len(results), 2)

    def test_search_pagination(self) -> None:
        entries = _make_entries(20, agent="page-test")
        self.logger.ingest_batch(entries)

        page1 = self.logger.search(agent="page-test", limit=5, offset=0, sort_desc=True)
        page2 = self.logger.search(agent="page-test", limit=5, offset=5, sort_desc=True)
        self.assertEqual(len(page1), 5)
        self.assertEqual(len(page2), 5)
        # Ensure no overlap
        ids1 = {id(e) for e in page1}
        ids2 = {id(e) for e in page2}
        self.assertEqual(ids1 & ids2, set())

    def test_search_time_range(self) -> None:
        now = time.time()
        old_entry = _make_entry(timestamp=now - 1000, message="old")
        new_entry = _make_entry(timestamp=now, message="new")
        self.logger.ingest(old_entry)
        self.logger.ingest(new_entry)

        results = self.logger.search(min_ts=now - 500)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].message, "new")

    def test_list_agents(self) -> None:
        self.logger.ingest(_make_entry(agent="a"))
        self.logger.ingest(_make_entry(agent="a"))
        self.logger.ingest(_make_entry(agent="b"))
        agents = self.logger.list_agents()
        self.assertEqual(agents["a"], 2)
        self.assertEqual(agents["b"], 1)

    def test_stats(self) -> None:
        for i in range(5):
            self.logger.ingest(_make_entry(
                level=["DEBUG", "INFO", "ERROR", "ERROR", "CRITICAL"][i]
            ))
        stats = self.logger.stats()
        self.assertEqual(stats["total_entries"], 5)
        self.assertEqual(stats["level_counts"]["ERROR"], 2)
        self.assertGreater(stats["earliest_ts"], 0)

    def test_prune(self) -> None:
        now = time.time()
        old = _make_entry(timestamp=now - (100 * 86400), message="ancient")
        recent = _make_entry(timestamp=now, message="fresh")
        self.logger.ingest(old)
        self.logger.ingest(recent)

        removed = self.logger.prune(days=30)
        self.assertEqual(removed, 1)
        all_entries = self.logger.all_entries()
        self.assertEqual(len(all_entries), 1)
        self.assertEqual(all_entries[0].message, "fresh")

    def test_persistence_jsonl(self) -> None:
        """Entries written to JSONL files can be re-read on rebuild."""
        self.logger.ingest(_make_entry(agent="persist-agent", message="saved"))
        self.logger.close()

        # Re-open a new logger on the same directory
        logger2 = FleetLogger(log_dir=self.log_dir)
        count = logger2.rebuild_index()
        self.assertEqual(count, 1)
        entries = logger2.search(agent="persist-agent")
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].message, "saved")
        logger2.close()

    def test_rebuild_index_multiple_agents(self) -> None:
        for agent in ["alpha", "beta", "gamma"]:
            for i in range(3):
                self.logger.ingest(_make_entry(agent=agent, message=f"{agent}-{i}"))
        self.logger.close()

        logger2 = FleetLogger(log_dir=self.log_dir)
        count = logger2.rebuild_index()
        self.assertEqual(count, 9)
        agents = logger2.list_agents()
        for agent in ["alpha", "beta", "gamma"]:
            self.assertEqual(agents[agent], 3)
        logger2.close()

    def test_tail_subscription(self) -> None:
        collected: list[LogEntry] = []

        def cb(entry: LogEntry) -> None:
            collected.append(entry)

        sub_id = self.logger.subscribe_tail(cb)
        self.logger.ingest(_make_entry(message="tail-test"))
        self.logger.unsubscribe_tail(sub_id)

        self.assertEqual(len(collected), 1)
        self.assertEqual(collected[0].message, "tail-test")

    def test_glob_agent_search(self) -> None:
        self.logger.ingest(_make_entry(agent="svc-auth"))
        self.logger.ingest(_make_entry(agent="svc-db"))
        self.logger.ingest(_make_entry(agent="web-frontend"))

        results = self.logger.search(agent="svc-*")
        self.assertEqual(len(results), 2)

    def test_rotation(self) -> None:
        """Log files exceeding max size get rotated."""
        tiny_logger = FleetLogger(log_dir=self.log_dir, max_file_bytes=200)
        # Ingest enough entries to exceed 200 bytes
        entries = _make_entries(20, agent="rotate-agent")
        tiny_logger.ingest_batch(entries)
        # Check that a rotated file exists
        rotated_files = list(Path(self.log_dir).glob("rotate-agent.*.jsonl"))
        # Rotation should have happened (file exceeds 200 bytes)
        self.assertGreater(len(rotated_files), 0)
        tiny_logger.close()


# ---------------------------------------------------------------------------
# Test: LogQuery engine
# ---------------------------------------------------------------------------

class TestLogQuery(unittest.TestCase):
    """Tests for the LogQuery engine."""

    def setUp(self) -> None:
        self.log_dir = _tmp_log_dir()
        self.fl = FleetLogger(log_dir=self.log_dir)
        self.q = LogQuery(self.fl)

    def tearDown(self) -> None:
        self.fl.close()
        shutil.rmtree(self.log_dir, ignore_errors=True)

    def test_simple_query_level(self) -> None:
        self.fl.ingest(_make_entry(level="ERROR", message="fail"))
        self.fl.ingest(_make_entry(level="DEBUG", message="noise"))
        result = self.q.search_simple("level:ERROR")
        self.assertEqual(result["count"], 1)

    def test_simple_query_agent(self) -> None:
        self.fl.ingest(_make_entry(agent="my-service", message="hello"))
        result = self.q.search_simple("agent:my-service")
        self.assertEqual(result["count"], 1)

    def test_simple_query_freeform(self) -> None:
        self.fl.ingest(_make_entry(message="connection refused on port 5432"))
        self.fl.ingest(_make_entry(message="all good"))
        result = self.q.search_simple("port 5432")
        self.assertEqual(result["count"], 1)

    def test_aggregate_by_level(self) -> None:
        for level in ["INFO", "INFO", "ERROR", "DEBUG", "ERROR", "CRITICAL"]:
            self.fl.ingest(_make_entry(level=level))
        result = self.q.aggregate(group_by="level")
        groups = {g[0]: g[1] for g in result["groups"]}
        self.assertEqual(groups["ERROR"], 2)
        self.assertEqual(groups["INFO"], 2)

    def test_aggregate_by_agent(self) -> None:
        for i in range(10):
            self.fl.ingest(_make_entry(agent=f"svc-{i % 3}"))
        result = self.q.aggregate(group_by="agent")
        self.assertEqual(len(result["groups"]), 3)
        total = sum(g[1] for g in result["groups"])
        self.assertEqual(total, 10)

    def test_export_json(self) -> None:
        self.fl.ingest(_make_entry(message="export me"))
        output = self.q.export(fmt="json")
        data = json.loads(output)
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 1)

    def test_export_csv(self) -> None:
        self.fl.ingest(_make_entry(agent="csv-test", message="row1"))
        output = self.q.export(fmt="csv")
        lines = output.strip().split("\n")
        self.assertEqual(len(lines), 2)  # header + 1 row
        self.assertIn("csv-test", lines[1])

    def test_export_text(self) -> None:
        self.fl.ingest(_make_entry(message="text export"))
        output = self.q.export(fmt="text")
        self.assertIn("text export", output)

    def test_trace(self) -> None:
        tid = "trace-xyz-789"
        self.fl.ingest(_make_entry(trace_id=tid, message="req start"))
        self.fl.ingest(_make_entry(trace_id="other", message="noise"))
        self.fl.ingest(_make_entry(trace_id=tid, message="req end"))
        result = self.q.trace(tid)
        self.assertEqual(result["count"], 2)
        self.assertEqual(result["entries"][0]["message"], "req start")


# ---------------------------------------------------------------------------
# Test: HTTP Server
# ---------------------------------------------------------------------------

class TestHTTPServer(unittest.TestCase):
    """Integration tests for the HTTP server endpoints.

    Starts a real server in a background thread and makes HTTP requests.
    """

    @classmethod
    def setUpClass(cls) -> None:
        cls.log_dir = _tmp_log_dir()
        cls.port = 18971  # use a non-standard port to avoid conflicts

        # Start the server in a background thread
        cls.server_thread = threading.Thread(
            target=serve,
            kwargs={
                "host": "127.0.0.1",
                "port": cls.port,
                "log_dir": cls.log_dir,
                "retention_days": 365,
            },
            daemon=True,
        )
        cls.server_thread.start()
        # Give the server time to start
        time.sleep(0.5)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.log_dir, ignore_errors=True)

    def _url(self, path: str) -> str:
        return f"http://127.0.0.1:{self.port}/{path}"

    def _get(self, path: str) -> Any:
        req = urllib.request.Request(self._url(path))
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode())

    def _post(self, path: str, data: Any) -> Any:
        body = json.dumps(data).encode()
        req = urllib.request.Request(
            self._url(path),
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode())

    def _delete(self, path: str) -> Any:
        req = urllib.request.Request(self._url(path), method="DELETE")
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode())

    def test_health(self) -> None:
        result = self._get("health")
        self.assertEqual(result["status"], "ok")
        self.assertIn("entries", result)
        self.assertIn("agents", result)

    def test_ingest_single(self) -> None:
        result = self._post("ingest", {
            "timestamp": time.time(),
            "agent": "http-test",
            "level": "INFO",
            "message": "ingested via HTTP",
        })
        self.assertEqual(result["ingested"], 1)
        self.assertEqual(result["status"], "ok")

    def test_ingest_batch(self) -> None:
        entries = []
        for i in range(5):
            entries.append({
                "timestamp": time.time() + i,
                "agent": "batch-test",
                "level": "DEBUG",
                "message": f"batch entry {i}",
            })
        result = self._post("ingest", entries)
        self.assertEqual(result["ingested"], 5)

    def test_search(self) -> None:
        # First ingest something
        self._post("ingest", {
            "timestamp": time.time(),
            "agent": "search-test",
            "level": "ERROR",
            "message": "searchable error from http",
            "trace_id": "http-trace-1",
        })
        result = self._get("search?agent=search-test&limit=10")
        self.assertIn("results", result)
        self.assertGreaterEqual(result["count"], 1)

    def test_search_by_level(self) -> None:
        result = self._get("search?level=ERROR&limit=100")
        self.assertIn("results", result)
        for entry in result["results"]:
            self.assertIn(entry["level"], {"ERROR", "CRITICAL"})

    def test_agents(self) -> None:
        result = self._get("agents")
        self.assertIn("agents", result)
        self.assertIn("count", result)

    def test_stats(self) -> None:
        result = self._get("stats")
        self.assertIn("total_entries", result)
        self.assertIn("level_counts", result)
        self.assertIn("agent_counts", result)

    def test_trace(self) -> None:
        tid = "http-trace-endpoint-test"
        for msg in ["start", "middle", "end"]:
            self._post("ingest", {
                "timestamp": time.time(),
                "agent": "tracer",
                "level": "INFO",
                "message": msg,
                "trace_id": tid,
            })
        result = self._get(f"trace/{tid}")
        self.assertEqual(result["trace_id"], tid)
        self.assertEqual(result["count"], 3)

    def test_prune(self) -> None:
        result = self._delete("prune?days=0")
        self.assertIn("pruned", result)

    def test_ingest_invalid_level(self) -> None:
        try:
            self._post("ingest", {
                "timestamp": time.time(),
                "agent": "bad",
                "level": "INVALID_LEVEL",
                "message": "bad level",
            })
            self.fail("Should have raised an HTTP error")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 400)
            body = json.loads(e.read().decode())
            self.assertIn("error", body)


# ---------------------------------------------------------------------------
# Test: Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases(unittest.TestCase):
    """Edge case and stress tests."""

    def setUp(self) -> None:
        self.log_dir = _tmp_log_dir()
        self.logger = FleetLogger(log_dir=self.log_dir)

    def tearDown(self) -> None:
        self.logger.close()
        shutil.rmtree(self.log_dir, ignore_errors=True)

    def test_empty_search(self) -> None:
        results = self.logger.search(agent="nonexistent")
        self.assertEqual(len(results), 0)

    def test_empty_stats(self) -> None:
        stats = self.logger.stats()
        self.assertEqual(stats["total_entries"], 0)
        self.assertEqual(stats["agents_count"], 0)

    def test_large_batch(self) -> None:
        entries = _make_entries(1000, agent="stress")
        count = self.logger.ingest_batch(entries)
        self.assertEqual(count, 1000)
        results = self.logger.search(agent="stress", limit=1000)
        self.assertEqual(len(results), 1000)

    def test_unicode_messages(self) -> None:
        msg = "🎉 Unicode log entry — 中文测试 — Ñoño café"
        self.logger.ingest(_make_entry(message=msg))
        results = self.logger.search(pattern=r"Unicode")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].message, msg)

    def test_empty_message(self) -> None:
        self.logger.ingest(_make_entry(message=""))
        results = self.logger.all_entries()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].message, "")

    def test_concurrent_ingest(self) -> None:
        """Multiple threads ingesting concurrently should not lose entries."""
        thread_count = 10
        entries_per_thread = 50
        errors: list[str] = []

        def ingest_batch(thread_id: int) -> None:
            try:
                entries = [
                    _make_entry(agent=f"concurrent-{thread_id}", message=f"msg-{i}")
                    for i in range(entries_per_thread)
                ]
                self.logger.ingest_batch(entries)
            except Exception as exc:
                errors.append(str(exc))

        threads = [
            threading.Thread(target=ingest_batch, args=(i,))
            for i in range(thread_count)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0, f"Concurrent errors: {errors}")
        stats = self.logger.stats()
        expected = thread_count * entries_per_thread
        self.assertEqual(stats["total_entries"], expected)


# ---------------------------------------------------------------------------
# Test: Index rebuild from corrupted JSONL
# ---------------------------------------------------------------------------

class TestCorruptedJSONL(unittest.TestCase):
    """Logger should skip malformed lines gracefully."""

    def setUp(self) -> None:
        self.log_dir = _tmp_log_dir()
        Path(self.log_dir).mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.log_dir, ignore_errors=True)

    def test_skips_bad_lines(self) -> None:
        # Write a JSONL file with some bad lines
        jsonl_path = Path(self.log_dir) / "corrupt-agent.jsonl"
        with open(jsonl_path, "w") as f:
            f.write('{"timestamp":1.0,"agent":"corrupt-agent","level":"INFO","message":"good"}\n')
            f.write("this is not json\n")
            f.write('{"timestamp":2.0,"agent":"corrupt-agent","level":"ERROR","message":"also good"}\n')
            f.write("\n")  # empty line
            f.write("{truncated\n")  # truncated JSON

        logger = FleetLogger(log_dir=self.log_dir)
        count = logger.rebuild_index()
        logger.close()

        # Should have loaded 2 valid entries
        self.assertEqual(count, 2)


if __name__ == "__main__":
    unittest.main()
