from contextvars import ContextVar
import json
import logging
import sys
from loguru import logger

# Thread-safe request scopes for logging traceability
request_id_context: ContextVar[str] = ContextVar("request_id", default="")
request_path_context: ContextVar[str] = ContextVar("request_path", default="")
request_method_context: ContextVar[str] = ContextVar("request_method", default="")
request_duration_context: ContextVar[float] = ContextVar("request_duration", default=0.0)

class InterceptHandler(logging.Handler):
    """Intercepts standard logging records and redirects them to Loguru."""
    def emit(self, record):
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())

def patch_logger(record):
    """Enriches the Loguru record with request-scoped tracing context variables."""
    req_id = request_id_context.get()
    path = request_path_context.get()
    method = request_method_context.get()
    duration = request_duration_context.get()

    if req_id:
        record["extra"]["request_id"] = req_id
    if path:
        record["extra"]["path"] = path
    if method:
        record["extra"]["method"] = method
    if duration:
        record["extra"]["duration_ms"] = round(duration, 2)

def json_serializer(message):
    """Enforces strict JSON schema consistency in production environments."""
    record = message.record
    extra = record["extra"]

    payload = {
        "timestamp": record["time"].isoformat(),
        "level": record["level"].name,
        "message": record["message"],
    }

    # Inject request tracing attributes
    if "request_id" in extra:
        payload["request_id"] = extra["request_id"]
    if "path" in extra:
        payload["path"] = extra["path"]
    if "method" in extra:
        payload["method"] = extra["method"]
    if "duration_ms" in extra:
        payload["duration_ms"] = extra["duration_ms"]

    if record["exception"]:
        payload["exception"] = {
            "type": record["exception"].type.__name__,
            "value": str(record["exception"].value),
        }

    sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    sys.stdout.flush()

def setup_logging():
    """Initializes Loguru configuration, interceptors, and strips default handlers."""
    logger.remove()

    from app.core.config import settings
    _is_dev = settings.ENV.lower() not in {"prod", "production", "staging"}

    logger.configure(patcher=patch_logger)

    if _is_dev:
        # Development readable console logs
        logger.add(
            sys.stdout,
            enqueue=True,
            backtrace=True,
            diagnose=True,
            format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
        )
    else:
        # Strict production structured formatting
        logger.add(json_serializer, enqueue=True)

    # Overwrite the standard logging configuration
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

    # Disable default uvicorn handlers to prevent log duplication
    for logger_name in ("uvicorn", "uvicorn.asgi", "uvicorn.access", "uvicorn.error"):
        logging_logger = logging.getLogger(logger_name)
        logging_logger.handlers = []
        logging_logger.propagate = True
