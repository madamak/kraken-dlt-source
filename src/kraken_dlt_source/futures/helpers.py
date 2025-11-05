"""Utility functions for timestamp handling, data transformation, and logging."""

import json
import logging
from datetime import UTC, datetime
from typing import Any, Dict, Mapping, MutableMapping, Optional, Sequence

LOGGER = logging.getLogger(__name__)

# Page size optimization for historical backfills:
# - account_log: max 100,000 but rate limits favor 5,000 (6 tokens vs 10)
# - executions/position_history: max unknown, testing shows 5,000 works well
DEFAULT_PAGE_SIZE = 500  # Conservative default for incremental syncs
BACKFILL_PAGE_SIZE = 5000  # Optimized for historical backfills

# Timestamp field mappings for extraction from nested records
EXECUTION_TIMESTAMP_FIELDS = ("timestamp", "info.timestamp", "takerOrder.timestamp", "trade.timestamp")
ACCOUNT_LOG_TIMESTAMP_FIELDS = ("timestamp", "date")
POSITION_HISTORY_TIMESTAMP_FIELDS = ("timestamp", "event.timestamp")


def extract_timestamp(record: Mapping[str, Any], fields: Sequence[str]) -> Optional[int]:
    """Extract timestamp in milliseconds from record using fallback field list.

    Parameters
    ----------
    record:
        Record dictionary to search.
    fields:
        Sequence of field paths (dot-notation) to try in order.

    Returns
    -------
    Optional[int]
        Timestamp in milliseconds, or None if not found.
    """
    for field in fields:
        value = lookup_nested(record, field)
        if value is None:
            continue
        ts = coerce_timestamp_ms(value)
        if ts is not None:
            return ts
    return None


def lookup_nested(record: Mapping[str, Any], field: str) -> Any:
    """Recursively look up nested field using dot notation.

    Parameters
    ----------
    record:
        Mapping to search.
    field:
        Dot-separated field path (e.g., "event.PositionUpdate.timestamp").

    Returns
    -------
    Any:
        Field value, or None if path doesn't exist.

    Examples
    --------
    >>> lookup_nested({"a": {"b": {"c": 123}}}, "a.b.c")
    123
    >>> lookup_nested({"a": {"b": None}}, "a.b.c")
    None
    """
    parts = field.split(".")
    current: Any = record
    for part in parts:
        if not isinstance(current, Mapping):
            return None
        current = current.get(part)
        if current is None:
            return None
    return current


def coerce_timestamp_ms(raw: Any) -> Optional[int]:
    """Convert various timestamp formats to milliseconds since epoch.

    Handles:
    - Integer/float milliseconds or seconds (auto-detected by magnitude)
    - ISO 8601 strings (e.g., "2024-01-15T10:30:00Z")
    - String representations of numbers

    Parameters
    ----------
    raw:
        Timestamp value in various formats.

    Returns
    -------
    Optional[int]
        Timestamp in milliseconds, or None if coercion fails.

    Examples
    --------
    >>> coerce_timestamp_ms(1705318200000)
    1705318200000
    >>> coerce_timestamp_ms(1705318200)  # seconds -> milliseconds
    1705318200000
    >>> coerce_timestamp_ms("2024-01-15T10:30:00Z")
    1705318200000
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        value = float(raw)
    elif isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            return None
        try:
            value = float(raw)
        except ValueError:
            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                return None
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            value = dt.timestamp()
            return int(value * 1000)
    else:
        return None

    # Heuristic: treat numbers greater than seconds threshold as milliseconds
    if value > 1_000_000_000_000:
        return int(value)
    return int(value * 1000)


def ms_to_iso(timestamp_ms: int) -> str:
    """Convert millisecond timestamp to ISO 8601 string.

    Parameters
    ----------
    timestamp_ms:
        Timestamp in milliseconds since epoch.

    Returns
    -------
    str:
        ISO 8601 formatted timestamp with 'Z' suffix (e.g., "2024-01-15T10:30:00Z").
    """
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC).isoformat().replace("+00:00", "Z")


def json_dumps(record: Mapping[str, Any]) -> str:
    """Serialize record to stable JSON string.

    Uses sorted keys and compact separators for consistent output.

    Parameters
    ----------
    record:
        Dictionary to serialize.

    Returns
    -------
    str:
        JSON string.
    """
    return json.dumps(record, sort_keys=True, separators=(",", ":"))


def initial_since(start_timestamp: Optional[str], state: MutableMapping[str, Any]) -> Optional[int]:
    """Determine initial 'since' timestamp from state or parameter.

    Parameters
    ----------
    start_timestamp:
        Optional start timestamp from function parameter (ISO or milliseconds).
    state:
        DLT resource state dictionary.

    Returns
    -------
    Optional[int]:
        Timestamp in milliseconds, or None if neither state nor parameter provided.
    """
    if state.get("last_timestamp"):
        return int(state["last_timestamp"])
    if not start_timestamp:
        return None
    ts = coerce_timestamp_ms(start_timestamp)
    return ts


def prepare_params(base_params: Optional[Mapping[str, Any]], since: Optional[int], token: Optional[str]) -> Dict[str, Any]:
    """Build API request parameters with optional 'since' and continuation_token.

    Parameters
    ----------
    base_params:
        Base parameters (e.g., {"count": 500}).
    since:
        Optional 'since' timestamp in milliseconds.
    token:
        Optional continuation token from previous response.

    Returns
    -------
    Dict[str, Any]:
        Merged parameters dictionary.
    """
    params: Dict[str, Any] = dict(base_params or {})
    if since:
        params["since"] = since
    if token:
        params["continuation_token"] = token
    return params


def log_resource_stats(name: str, count: int, last_timestamp: Optional[Any]) -> None:
    """Log resource loading summary.

    Parameters
    ----------
    name:
        Resource name.
    count:
        Number of records loaded.
    last_timestamp:
        Last timestamp cursor value (if applicable).
    """
    LOGGER.info(
        "Resource %s loaded %s rows%s",
        name,
        count,
        f" (last_timestamp={last_timestamp})" if last_timestamp is not None else "",
    )


def enrich_record(
    element: Mapping[str, Any],
    timestamp_ms: int,
) -> Dict[str, Any]:
    """Enrich record with cursor metadata and raw_data field.

    Parameters
    ----------
    element:
        Original record from API.
    timestamp_ms:
        Timestamp in milliseconds for cursor tracking.

    Returns
    -------
    Dict[str, Any]:
        Enriched record with _cursor_timestamp_ms, _cursor_timestamp, and raw_data fields.
    """
    record = dict(element)
    record["_cursor_timestamp_ms"] = timestamp_ms
    record["_cursor_timestamp"] = ms_to_iso(timestamp_ms)
    record["raw_data"] = json_dumps(element)
    return record


__all__ = [
    "DEFAULT_PAGE_SIZE",
    "BACKFILL_PAGE_SIZE",
    "EXECUTION_TIMESTAMP_FIELDS",
    "ACCOUNT_LOG_TIMESTAMP_FIELDS",
    "POSITION_HISTORY_TIMESTAMP_FIELDS",
    "extract_timestamp",
    "lookup_nested",
    "coerce_timestamp_ms",
    "ms_to_iso",
    "json_dumps",
    "initial_since",
    "prepare_params",
    "log_resource_stats",
    "enrich_record",
]
