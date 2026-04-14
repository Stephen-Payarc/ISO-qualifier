"""
Disk-based result cache backed by diskcache.

Keyed by a stable string (e.g. contact ID or URL hash). Both agent stages
write results here so re-runs skip already-processed contacts without
hitting the network or spending API tokens.
"""

import hashlib
import json
from pathlib import Path
from typing import Any

import diskcache

from config import settings


_cache: diskcache.Cache | None = None


def _get_cache() -> diskcache.Cache:
    global _cache
    if _cache is None:
        _cache = diskcache.Cache(str(settings.CACHE_DIR), size_limit=2**32)  # 4 GB cap
    return _cache


def make_key(*parts: str) -> str:
    """Build a stable cache key from one or more string parts."""
    combined = "|".join(p.strip().lower() for p in parts if p)
    return hashlib.sha256(combined.encode()).hexdigest()


def get(key: str) -> Any | None:
    """Return cached value or None if not present."""
    return _get_cache().get(key)


def set(key: str, value: Any) -> None:
    """Store value under key. Value must be JSON-serialisable."""
    _get_cache().set(key, value)


def exists(key: str) -> bool:
    return key in _get_cache()


def delete(key: str) -> None:
    _get_cache().delete(key)


def clear() -> None:
    """Wipe the entire cache. Use with care."""
    _get_cache().clear()


def stats() -> dict:
    c = _get_cache()
    return {"size": len(c), "volume_bytes": c.volume()}
