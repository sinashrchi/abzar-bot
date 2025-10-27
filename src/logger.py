# src/logger.py
import json
import sys
import time


def log(level: str, event: str, meta=None, user=None, module=None):
    """
    لاگ ساخت‌یافته‌ی ساده به‌صورت JSON روی stdout.
    """
    rec = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "level": (level or "INFO").upper(),
        "event": event,
        "user": user,
        "module": module or __name__,
        "meta": meta or {},
    }
    sys.stdout.write(json.dumps(rec, ensure_ascii=False) + "\n")
    sys.stdout.flush()
