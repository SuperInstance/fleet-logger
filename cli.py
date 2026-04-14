#!/usr/bin/env python3
"""Fleet Logger CLI — command-line interface for the centralized logging agent.

Usage:
    python cli.py serve [--port PORT] [--log-dir DIR] [--retention DAYS]
    python cli.py search "<query>"
    python cli.py tail [--agent NAME] [--level LEVEL] [--timeout SEC]
    python cli.py stats
    python cli.py trace <trace_id>
    python cli.py export [--format json|csv|text] [--output FILE]
    python cli.py prune --days N
    python cli.py onboard
"""

from __future__ import annotations

import argparse
import json
import sys
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from logger import (
    DEFAULT_LOG_DIR,
    DEFAULT_PORT,
    DEFAULT_RETENTION_DAYS,
    FleetLogger,
    LogEntry,
)
from query import LogQuery


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------

def cmd_serve(args: argparse.Namespace) -> None:
    """Start the Fleet Logger HTTP server."""
    from server import serve

    port = args.port or DEFAULT_PORT
    log_dir = args.log_dir or DEFAULT_LOG_DIR
    retention = args.retention or DEFAULT_RETENTION_DAYS
    serve(host="0.0.0.0", port=port, log_dir=log_dir, retention_days=retention)


def cmd_search(args: argparse.Namespace) -> None:
    """Search logs from local storage."""
    fl = _make_logger(args)
    q = LogQuery(fl)

    if args.query:
        result = q.search_simple(args.query, limit=args.limit)
    else:
        result = q.search(
            agent=args.agent,
            level=args.level,
            pattern=args.pattern,
            limit=args.limit,
        )

    _print_json(result)


def cmd_tail(args: argparse.Namespace) -> None:
    """Follow logs in real-time (like tail -f)."""
    fl = _make_logger(args)
    q = LogQuery(fl)

    print("[fleet-logger] Tailing logs… (Ctrl+C to stop)")
    try:
        while True:
            entries = q.tail(
                agent=args.agent,
                level=args.level,
                timeout=5.0,
            )
            for e in entries:
                ts = datetime.fromtimestamp(e.timestamp, tz=timezone.utc).isoformat()
                print(f"[{ts}] [{e.level:8s}] {e.agent}: {e.message}")
    except KeyboardInterrupt:
        print("\n[fleet-logger] Tail stopped.")
    finally:
        fl.close()


def cmd_stats(args: argparse.Namespace) -> None:
    """Show log statistics."""
    fl = _make_logger(args)
    try:
        stats = fl.stats()
        _print_json(stats)
    finally:
        fl.close()


def cmd_trace(args: argparse.Namespace) -> None:
    """Show all logs for a given trace ID."""
    fl = _make_logger(args)
    q = LogQuery(fl)
    try:
        result = q.trace(args.trace_id)
        _print_json(result)
    finally:
        fl.close()


def cmd_export(args: argparse.Namespace) -> None:
    """Export logs to a file."""
    fl = _make_logger(args)
    q = LogQuery(fl)
    try:
        output = q.export(
            fmt=args.format,
            agent=args.agent,
            level=args.level,
            pattern=args.pattern,
            limit=args.limit,
        )
        if args.output:
            Path(args.output).write_text(output, encoding="utf-8")
            print(f"[fleet-logger] Exported to {args.output}")
        else:
            print(output)
    finally:
        fl.close()


def cmd_prune(args: argparse.Namespace) -> None:
    """Delete logs older than N days."""
    fl = _make_logger(args)
    try:
        removed = fl.prune(args.days)
        print(f"[fleet-logger] Pruned {removed} entries older than {args.days} days.")
    finally:
        fl.close()


def cmd_onboard(args: argparse.Namespace) -> None:
    """Set up the Fleet Logger directory and config."""
    log_dir = Path(args.log_dir or DEFAULT_LOG_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)

    config_file = log_dir / "config.json"
    config = {
        "log_dir": str(log_dir),
        "retention_days": args.retention or DEFAULT_RETENTION_DAYS,
        "max_file_bytes": 50 * 1024 * 1024,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    config_file.write_text(json.dumps(config, indent=2), encoding="utf-8")

    print(f"[fleet-logger] Onboarded! Log directory: {log_dir}")
    print(f"[fleet-logger] Config: {config_file}")
    print(f"[fleet-logger] Retention: {config['retention_days']} days")
    print("[fleet-logger] Ready. Run 'python cli.py serve' to start.")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_logger(args: argparse.Namespace) -> FleetLogger:
    log_dir = getattr(args, "log_dir", None) or DEFAULT_LOG_DIR
    retention = getattr(args, "retention", None) or DEFAULT_RETENTION_DAYS
    fl = FleetLogger(log_dir=log_dir, retention_days=retention)
    fl.rebuild_index()
    return fl


def _print_json(data: object) -> None:
    print(json.dumps(data, indent=2, ensure_ascii=False))


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fleet-logger",
        description="Centralized structured logging for the Pelagic fleet.",
    )
    parser.add_argument(
        "--log-dir",
        default=None,
        help=f"Log storage directory (default: {DEFAULT_LOG_DIR})",
    )
    parser.add_argument(
        "--retention",
        type=int,
        default=None,
        help=f"Retention in days (default: {DEFAULT_RETENTION_DAYS})",
    )

    sub = parser.add_subparsers(dest="command", help="Available commands")

    # serve
    p_serve = sub.add_parser("serve", help="Start the HTTP server")
    p_serve.add_argument("--port", type=int, default=DEFAULT_PORT)
    p_serve.set_defaults(func=cmd_serve)

    # search
    p_search = sub.add_parser("search", help="Search logs")
    p_search.add_argument("query", nargs="?", help="Search query string")
    p_search.add_argument("--agent", help="Filter by agent name")
    p_search.add_argument("--level", help="Filter by minimum level")
    p_search.add_argument("--pattern", help="Regex pattern for message")
    p_search.add_argument("--limit", type=int, default=50)
    p_search.set_defaults(func=cmd_search)

    # tail
    p_tail = sub.add_parser("tail", help="Follow logs in real-time")
    p_tail.add_argument("--agent", help="Filter by agent")
    p_tail.add_argument("--level", help="Filter by minimum level")
    p_tail.add_argument("--timeout", type=float, default=5.0)
    p_tail.set_defaults(func=cmd_tail)

    # stats
    sub.add_parser("stats", help="Show log statistics").set_defaults(func=cmd_stats)

    # trace
    p_trace = sub.add_parser("trace", help="Show trace by ID")
    p_trace.add_argument("trace_id", help="Trace ID to look up")
    p_trace.set_defaults(func=cmd_trace)

    # export
    p_export = sub.add_parser("export", help="Export logs")
    p_export.add_argument("--format", choices=["json", "csv", "text"], default="json")
    p_export.add_argument("--output", help="Output file path")
    p_export.add_argument("--agent", help="Filter by agent")
    p_export.add_argument("--level", help="Filter by minimum level")
    p_export.add_argument("--pattern", help="Regex pattern")
    p_export.add_argument("--limit", type=int, default=10_000)
    p_export.set_defaults(func=cmd_export)

    # prune
    p_prune = sub.add_parser("prune", help="Delete old logs")
    p_prune.add_argument("--days", type=int, required=True, help="Days to retain")
    p_prune.set_defaults(func=cmd_prune)

    # onboard
    sub.add_parser("onboard", help="Set up the logger").set_defaults(func=cmd_onboard)

    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
