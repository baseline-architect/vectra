import json
import logging
import sys
from typing import Any, Mapping


class JsonFormatter(logging.Formatter):
    DEFAULT_KEYS = {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "level": record.levelname,
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "name": record.name,
            "message": record.getMessage(),
        }
        # Include any custom extras passed via logging `extra={...}`
        for key, value in record.__dict__.items():
            if key not in self.DEFAULT_KEYS and key not in payload:
                try:
                    json.dumps(value)  # validate serializable
                    payload[key] = value
                except TypeError:
                    payload[key] = str(value)
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    root.setLevel(level.upper())

    # Clear existing handlers to avoid duplicate logs on reload
    for h in list(root.handlers):
        root.removeHandler(h)

    json_formatter = JsonFormatter()

    stdout_handler = logging.StreamHandler(stream=sys.stdout)
    stdout_handler.setLevel(level.upper())
    stdout_handler.setFormatter(json_formatter)

    error_handler = logging.FileHandler("error.log")
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(json_formatter)

    root.addHandler(stdout_handler)
    root.addHandler(error_handler)
