import json
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Dict, Iterable, Iterator, Mapping, MutableMapping, Optional, Sequence, Tuple

import dlt
from dlt.sources.helpers import requests
from requests import exceptions as requests_exceptions

from .auth import KrakenFuturesAuth

BASE_URL = "https://futures.kraken.com"

# Page size optimization for historical backfills:
# - account_log: max 100,000 but rate limits favor 5,000 (6 tokens vs 10)
# - executions/position_history: max unknown, testing shows 5,000 works well
DEFAULT_PAGE_SIZE = 500  # Conservative default for incremental syncs
BACKFILL_PAGE_SIZE = 5000  # Optimized for historical backfills

EXECUTION_TIMESTAMP_FIELDS = ("timestamp", "info.timestamp", "takerOrder.timestamp", "trade.timestamp")
ACCOUNT_LOG_TIMESTAMP_FIELDS = ("timestamp", "date")
POSITION_HISTORY_TIMESTAMP_FIELDS = ("timestamp", "event.timestamp")


ALL_RESOURCE_NAMES = ("executions", "account_log", "position_history", "tickers", "open_positions")

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class KrakenFuturesClient:
    auth: Optional[KrakenFuturesAuth] = None
    session: Optional[requests.Session] = None
    max_retries: int = 3
    initial_delay: float = 2.0  # Increased from 1.0 to match working implementation
    backoff_factor: float = 2.0
    jitter_ratio: float = 0.1
    timeout: float = 30.0
    min_request_interval: float = 0.5  # Minimum delay between successful requests to avoid rate limits

    def __post_init__(self) -> None:
        if self.session is None:
            self.session = requests.Session()

    def get(
        self,
        path: str,
        params: Optional[Mapping[str, Any]] = None,
        *,
        private: bool = False,
    ) -> Mapping[str, Any]:
        # For derivatives endpoints, strip /derivatives prefix for auth signing
        auth_path = path.replace("/derivatives", "") if path.startswith("/derivatives") else path

        # Build query string manually to ensure consistent encoding between auth and request
        from urllib.parse import urlencode
        query_string = ""
        if params:
            # Filter out None values and convert to strings
            filtered = {k: str(v) for k, v in params.items() if v is not None}
            if filtered:
                query_string = urlencode(sorted(filtered.items()))

        # Build full URL with query string
        url = f"{BASE_URL}{path}"
        if query_string:
            url = f"{url}?{query_string}"

        headers: Dict[str, str] = {}
        if private:
            if not self.auth:
                raise ValueError("Private endpoints require KrakenFuturesAuth")
            # Pass params dict to auth for signature generation
            headers.update(self.auth(auth_path, params))

        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                # Don't pass params to requests.get since we already added them to URL
                response = self.session.get(url, headers=headers, timeout=self.timeout)

                if response.status_code == 429:
                    raise _RateLimitedError("HTTP 429 Too Many Requests", response=response)

                response.raise_for_status()
                payload = response.json()
                try:
                    _raise_if_error(payload)
                except RuntimeError as err:
                    message = str(err).lower()
                    if "permission" in message:
                        LOGGER.warning(
                            "Permission denied for %s; returning empty result.",
                            path,
                        )
                        return {}
                    raise
                if attempt:
                    LOGGER.info(
                        "Recovered after retry %s for %s", attempt, path
                    )
                # Add small delay after successful request to avoid rate limits
                if self.min_request_interval > 0:
                    time.sleep(self.min_request_interval)
                return payload
            except (_RateLimitedError, requests_exceptions.RequestException, ValueError) as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    break
                delay = self._compute_delay(attempt)
                LOGGER.warning(
                    "Retrying %s due to %s (attempt %s/%s, delay %.2fs)",
                    path,
                    exc,
                    attempt + 1,
                    self.max_retries,
                    delay,
                )
                time.sleep(delay)
                continue

        raise RuntimeError(f"Failed to fetch {path}: {last_error}")

    def _compute_delay(self, attempt: int) -> float:
        base = self.initial_delay * (self.backoff_factor ** attempt)
        jitter = base * self.jitter_ratio * random.uniform(-1, 1)
        return max(0.0, base + jitter)


class _RateLimitedError(Exception):
    def __init__(self, message: str, *, response: Optional[requests.Response] = None) -> None:
        super().__init__(message)
        self.response = response


def _raise_if_error(payload: Mapping[str, Any]) -> None:
    if payload is None:
        raise ValueError("Empty response payload")
    # Handle common patterns returned by Kraken Futures
    if isinstance(payload.get("result"), str) and payload["result"].lower() == "error":
        error = payload.get("error") or payload
        raise RuntimeError(f"Kraken Futures API error: {error}")
    if payload.get("success") is False:
        raise RuntimeError(f"Kraken Futures API error: {payload}")


def _extract_timestamp(record: Mapping[str, Any], fields: Sequence[str]) -> Optional[int]:
    for field in fields:
        value = _lookup(record, field)
        if value is None:
            continue
        ts = _coerce_timestamp_ms(value)
        if ts is not None:
            return ts
    return None


def _lookup(record: Mapping[str, Any], field: str) -> Any:
    parts = field.split(".")
    current: Any = record
    for part in parts:
        if not isinstance(current, Mapping):
            return None
        current = current.get(part)
        if current is None:
            return None
    return current


def _coerce_timestamp_ms(raw: Any) -> Optional[int]:
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


def _ms_to_iso(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC).isoformat().replace("+00:00", "Z")


def _json_dumps(record: Mapping[str, Any]) -> str:
    return json.dumps(record, sort_keys=True, separators=(",", ":"))


def _initial_since(start_timestamp: Optional[str], state: MutableMapping[str, Any]) -> Optional[int]:
    if state.get("last_timestamp"):
        return int(state["last_timestamp"])
    if not start_timestamp:
        return None
    ts = _coerce_timestamp_ms(start_timestamp)
    return ts


def _prepare_params(base_params: Optional[Mapping[str, Any]], since: Optional[int], token: Optional[str]) -> Dict[str, Any]:
    params: Dict[str, Any] = dict(base_params or {})
    if since:
        params["since"] = since
    if token:
        params["continuation_token"] = token
    return params


@dlt.resource(
    name="executions",
    primary_key="uid",
    write_disposition="append",
    columns={
        "event__execution__execution__order_data__fee_calculation_info__user_fee_discount_applied": {
            "data_type": "double",
            "nullable": True,
        },
        "event__execution__execution__order_data__fee_calculation_info__market_share_rebate_credited": {
            "data_type": "double",
            "nullable": True,
        },
    },
)
def executions(
    auth: Optional[KrakenFuturesAuth],
    start_timestamp: Optional[str] = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    client: Optional[KrakenFuturesClient] = None,
) -> Iterator[Mapping[str, Any]]:
    state = dlt.current.resource_state()
    client = client or KrakenFuturesClient(auth=auth)

    since = _initial_since(start_timestamp, state)
    token = state.get("continuation_token")

    max_timestamp_seen = since

    records_emitted = 0

    while True:
        params = _prepare_params({"count": page_size}, since, token)
        payload = client.get("/api/history/v2/executions", params=params, private=True)

        elements = payload.get("elements") or payload.get("executionEvents") or []
        next_token = payload.get("continuationToken") or payload.get("continuation_token")

        if not elements:
            state["continuation_token"] = None
            if since:
                state["last_timestamp"] = since
            _log_resource_stats("executions", records_emitted, state.get("last_timestamp"))
            break

        for element in elements:
            timestamp_ms = _extract_timestamp(element, EXECUTION_TIMESTAMP_FIELDS)
            if timestamp_ms is None:
                continue
            if not max_timestamp_seen or timestamp_ms > max_timestamp_seen:
                max_timestamp_seen = timestamp_ms
            record = dict(element)
            record["_cursor_timestamp_ms"] = timestamp_ms
            record["_cursor_timestamp"] = _ms_to_iso(timestamp_ms)
            record["raw_data"] = _json_dumps(element)
            yield record
            records_emitted += 1

        if next_token:
            token = next_token
            state["continuation_token"] = next_token
            continue

        if max_timestamp_seen:
            state["last_timestamp"] = str(max_timestamp_seen)
        state["continuation_token"] = None
        _log_resource_stats("executions", records_emitted, state.get("last_timestamp"))
        break


@dlt.resource(
    name="account_log",
    primary_key="booking_uid",
    write_disposition="append",
    columns={
        "collateral": {"data_type": "double", "nullable": True},
        "liquidation_fee": {"data_type": "double", "nullable": True},
        "position_uid": {"data_type": "text", "nullable": True},
    },
)
def account_log(
    auth: Optional[KrakenFuturesAuth],
    start_timestamp: Optional[str] = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    client: Optional[KrakenFuturesClient] = None,
) -> Iterator[Mapping[str, Any]]:
    if not auth:
        raise ValueError("account_log resource requires authentication")

    state = dlt.current.resource_state()
    client = client or KrakenFuturesClient(auth=auth)

    since = _initial_since(start_timestamp, state)
    token = state.get("continuation_token")
    before = state.get("before")

    max_timestamp_seen = since

    records_emitted = 0

    # Infinite loop detection: track recent 'before' values
    seen_before_timestamps = []
    max_before_history = 3  # Allow same timestamp to appear max 3 times

    while True:
        params = {"count": page_size}
        if before:
            params["before"] = before
        params = _prepare_params(params, since, token)

        payload = client.get("/api/history/v2/account-log", params=params, private=True)
        logs = payload.get("logs") or payload.get("accountLog") or []
        next_token = payload.get("continuationToken") or payload.get("continuation_token")

        if not logs:
            state["continuation_token"] = None
            state["before"] = None
            if since:
                state["last_timestamp"] = since
            LOGGER.info("account_log: No more records returned by API")
            _log_resource_stats("account_log", records_emitted, state.get("last_timestamp"))
            break

        timestamps: list[Tuple[int, Mapping[str, Any]]] = []
        for log in logs:
            timestamp_ms = _extract_timestamp(log, ACCOUNT_LOG_TIMESTAMP_FIELDS)
            if timestamp_ms is None:
                continue
            timestamps.append((timestamp_ms, log))
            if not max_timestamp_seen or timestamp_ms > max_timestamp_seen:
                max_timestamp_seen = timestamp_ms
            record = dict(log)
            record["_cursor_timestamp_ms"] = timestamp_ms
            record["_cursor_timestamp"] = _ms_to_iso(timestamp_ms)
            record["raw_data"] = _json_dumps(log)
            yield record
            records_emitted += 1

        if next_token:
            token = next_token
            state["continuation_token"] = next_token
            state["before"] = None
            seen_before_timestamps.clear()  # Reset loop detection on continuation token
            LOGGER.debug("account_log: Continuation token received, fetching next page")
            continue

        if max_timestamp_seen:
            state["last_timestamp"] = str(max_timestamp_seen)
        state["continuation_token"] = None

        # Check if we've reached the start timestamp
        if since and timestamps:
            earliest = min(ts for ts, _ in timestamps)
            if earliest <= since:
                LOGGER.info(
                    "account_log: Reached start timestamp (since=%s, earliest=%s). Backfill complete.",
                    _ms_to_iso(since),
                    _ms_to_iso(earliest),
                )
                state["before"] = None
                _log_resource_stats("account_log", records_emitted, state.get("last_timestamp"))
                break

        # Continue pagination if page is full
        if len(logs) >= page_size:
            earliest = min(ts for ts, _ in timestamps) if timestamps else None
            if earliest:
                # Detect infinite loop
                seen_before_timestamps.append(earliest)
                if len(seen_before_timestamps) > max_before_history:
                    seen_before_timestamps.pop(0)

                if len(seen_before_timestamps) == max_before_history and len(set(seen_before_timestamps)) == 1:
                    LOGGER.warning(
                        "account_log: Detected infinite loop at timestamp %s. "
                        "Same timestamp returned %d times. Stopping pagination.",
                        _ms_to_iso(earliest),
                        max_before_history,
                    )
                    state["before"] = None
                    _log_resource_stats("account_log", records_emitted, state.get("last_timestamp"))
                    break

                before = earliest
                state["before"] = str(before)
                token = None
                LOGGER.debug(
                    "account_log: Using fallback pagination, before=%s, records_so_far=%d",
                    _ms_to_iso(earliest),
                    records_emitted,
                )
                continue

        # Log why we stopped
        if timestamps:
            earliest_in_page = min(ts for ts, _ in timestamps)
            LOGGER.info(
                "account_log: Pagination complete - last page was partial (%d < %d records). "
                "Earliest timestamp reached: %s",
                len(logs),
                page_size,
                _ms_to_iso(earliest_in_page),
            )
        else:
            LOGGER.info(
                "account_log: Pagination complete - last page was partial (%d < %d records)",
                len(logs),
                page_size,
            )

        state["before"] = None
        _log_resource_stats("account_log", records_emitted, state.get("last_timestamp"))
        break


@dlt.resource(name="position_history", primary_key=["executionUid", "uid"], write_disposition="append")
def position_history(
    auth: Optional[KrakenFuturesAuth],
    start_timestamp: Optional[str] = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    client: Optional[KrakenFuturesClient] = None,
) -> Iterator[Mapping[str, Any]]:
    if not auth:
        raise ValueError("position_history resource requires authentication")

    state = dlt.current.resource_state()
    client = client or KrakenFuturesClient(auth=auth)

    since = _initial_since(start_timestamp, state)
    token = state.get("continuation_token")

    max_timestamp_seen = since

    records_emitted = 0

    while True:
        params = _prepare_params({"count": page_size}, since, token)
        payload = client.get("/api/history/v3/positions", params=params, private=True)

        elements = payload.get("elements") or []
        next_token = payload.get("continuationToken") or payload.get("continuation_token")

        if not elements:
            state["continuation_token"] = None
            if since:
                state["last_timestamp"] = since
            _log_resource_stats("position_history", records_emitted, state.get("last_timestamp"))
            break

        for element in elements:
            # Extract the PositionUpdate event while preserving uid and timestamp from outer element
            event = element.get("event") if isinstance(element, Mapping) else None
            if event and "PositionUpdate" in event:
                position_update = dict(event["PositionUpdate"])
                # Preserve outer-level fields (uid, timestamp) alongside the PositionUpdate data
                normalized = {
                    "uid": element.get("uid"),
                    "timestamp": element.get("timestamp"),
                    **position_update
                }
            else:
                normalized = dict(element)

            timestamp_ms = _extract_timestamp(normalized, POSITION_HISTORY_TIMESTAMP_FIELDS)
            if timestamp_ms is None:
                continue

            if not max_timestamp_seen or timestamp_ms > max_timestamp_seen:
                max_timestamp_seen = timestamp_ms

            record = dict(normalized)

            # Ensure executionUid exists for primary key constraint
            # For funding events without executionUid, generate a synthetic one
            if "executionUid" not in record or not record["executionUid"]:
                update_reason = record.get("updateReason", "unknown")
                tradeable = record.get("tradeable", "unknown")
                record["executionUid"] = f"{update_reason}-{tradeable}-{timestamp_ms}"

            record["_cursor_timestamp_ms"] = timestamp_ms
            record["_cursor_timestamp"] = _ms_to_iso(timestamp_ms)
            record["raw_data"] = _json_dumps(normalized)
            yield record
            records_emitted += 1

        if next_token:
            token = next_token
            state["continuation_token"] = next_token
            continue

        if max_timestamp_seen:
            state["last_timestamp"] = str(max_timestamp_seen)
        state["continuation_token"] = None
        _log_resource_stats("position_history", records_emitted, state.get("last_timestamp"))
        break


@dlt.resource(name="tickers", write_disposition="replace")
def tickers(client: Optional[KrakenFuturesClient] = None) -> Iterable[Mapping[str, Any]]:
    client = client or KrakenFuturesClient()
    payload = client.get("/derivatives/api/v3/tickers", params=None, private=False)
    tickers_data = payload.get("tickers") or []

    count = 0
    for ticker in tickers_data:
        record = dict(ticker)
        record["raw_data"] = _json_dumps(ticker)
        yield record
        count += 1

    _log_resource_stats("tickers", count, None)


@dlt.resource(name="open_positions", write_disposition="replace")
def open_positions(
    auth: Optional[KrakenFuturesAuth],
    client: Optional[KrakenFuturesClient] = None,
) -> Iterable[Mapping[str, Any]]:
    if not auth:
        raise ValueError("open_positions resource requires authentication")

    client = client or KrakenFuturesClient(auth=auth)
    payload = client.get("/derivatives/api/v3/openpositions", params=None, private=True)
    positions = payload.get("openPositions") or payload.get("openpositions") or []

    count = 0
    for position in positions:
        record = dict(position)
        record["raw_data"] = _json_dumps(position)
        yield record
        count += 1

    _log_resource_stats("open_positions", count, None)


def _log_resource_stats(name: str, count: int, last_timestamp: Optional[Any]) -> None:
    LOGGER.info(
        "Resource %s loaded %s rows%s",
        name,
        count,
        f" (last_timestamp={last_timestamp})" if last_timestamp is not None else "",
    )


__all__ = [
    "executions",
    "account_log",
    "position_history",
    "tickers",
    "open_positions",
    "KrakenFuturesClient",
    "ALL_RESOURCE_NAMES",
]
