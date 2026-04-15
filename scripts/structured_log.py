"""
Structured JSON logging.

Replaces ``logging.basicConfig(format="%(message)s")`` with a formatter
that emits one JSON object per line. JSON logs survive grep/jq pipelines
much better than the human-formatted lines we used to dump into
``data/logs/``, and they're easy to ingest into a hosted log store later
(Loki, Datadog, etc.) without having to re-parse string formats.

Usage:
    from scripts.structured_log import configure
    configure(log_file="data/logs/sync.jsonl", level="INFO")
    logger.info("syncing", extra={"sport": "mlb", "games": 14})

Each line looks like:
    {"ts": "2026-04-15T03:12:09.412Z", "level": "INFO",
     "logger": "scrapers.mlb_stats", "msg": "syncing",
     "sport": "mlb", "games": 14}

Fields passed via ``extra=`` are merged into the JSON object (top-level
keys), so consumers can filter on structured attributes rather than
parsing the message string.
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

# Built-in LogRecord attrs we don't want to repeat in the payload.
_RESERVED = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "message", "asctime", "taskName",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc)
                  .isoformat(timespec="milliseconds")
                  .replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # Merge any structured attrs the caller passed via extra=
        for key, val in record.__dict__.items():
            if key in _RESERVED:
                continue
            try:
                json.dumps(val)
                payload[key] = val
            except (TypeError, ValueError):
                payload[key] = repr(val)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure(log_file: str | None = None,
               level: str = "INFO",
               max_bytes: int = 5_000_000,
               backup_count: int = 5,
               also_stderr: bool = True) -> None:
    """Install JSON logging on the root logger.

    Idempotent: calling twice replaces the previous handlers so the test
    suite and CLI tools don't double-emit each line.
    """
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)

    formatter = JsonFormatter()
    if also_stderr:
        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(formatter)
        root.addHandler(sh)

    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        fh = RotatingFileHandler(
            log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8",
        )
        fh.setFormatter(formatter)
        root.addHandler(fh)

    root.setLevel(getattr(logging, level.upper(), logging.INFO))


def configure_from_env() -> None:
    """Honor LOG_FILE / LOG_LEVEL env vars; safe to call from any entry point."""
    configure(
        log_file=os.environ.get("LOG_FILE"),
        level=os.environ.get("LOG_LEVEL", "INFO"),
    )
