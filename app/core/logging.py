import json
import logging
import sys
import traceback
from datetime import datetime, timezone


class _JsonFormatter(logging.Formatter):
    """Emit one JSON object per log line — machine-readable by CloudWatch / Datadog / Loki."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }

        # Thread / task context
        if record.thread:
            payload["thread"] = record.thread

        # Include exc_info when present
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        elif record.exc_text:
            payload["exc"] = record.exc_text

        # Include stack_info when present
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)

        # Carry over any extra fields the caller passed via `extra={}`
        _standard = {
            "name", "msg", "args", "levelname", "levelno", "pathname",
            "filename", "module", "exc_info", "exc_text", "stack_info",
            "lineno", "funcName", "created", "msecs", "relativeCreated",
            "thread", "threadName", "processName", "process", "taskName",
            "message",
        }
        for key, val in record.__dict__.items():
            if key not in _standard and not key.startswith("_"):
                try:
                    json.dumps(val)  # only include JSON-serialisable extras
                    payload[key] = val
                except (TypeError, ValueError):
                    payload[key] = str(val)

        return json.dumps(payload, ensure_ascii=False)


def setup_logging():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])
