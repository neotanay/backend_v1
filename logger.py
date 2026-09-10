import os
import sys
from datetime import datetime, timezone

LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "WARN": 30, "ERROR": 40}
_configured_level = os.getenv("LOG_LEVEL", "INFO").upper()
_threshold = LEVELS.get(_configured_level, LEVELS["INFO"])


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _log(level_name: str, level_value: int, args) -> None:
    if level_value < _threshold:
        return
    parts = [_ts(), level_name] + [str(a) for a in args]
    line = " ".join(parts)
    stream = sys.stderr if level_value >= LEVELS["WARNING"] else sys.stdout
    print(line, file=stream)


def debug(*args):
    _log("DEBUG", LEVELS["DEBUG"], args)


def info(*args):
    _log("INFO", LEVELS["INFO"], args)


def warn(*args):
    _log("WARNING", LEVELS["WARNING"], args)


def error(*args):
    _log("ERROR", LEVELS["ERROR"], args)
