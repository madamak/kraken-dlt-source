"""Account log resource - incremental sync of account activity with hybrid pagination."""

import logging
from typing import Any, Iterator, Mapping, Optional, Tuple

import dlt

from ..client import KrakenFuturesClient
from ..auth import KrakenFuturesAuth
from ..helpers import (
    DEFAULT_PAGE_SIZE,
    ACCOUNT_LOG_TIMESTAMP_FIELDS,
    extract_timestamp,
    initial_since,
    prepare_params,
    log_resource_stats,
    enrich_record,
    ms_to_iso,
)

LOGGER = logging.getLogger(__name__)


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
    """Load account activity log with hybrid pagination strategy.

    This resource uses a two-tier pagination approach:
    1. Primary: continuation_token-based pagination (when available)
    2. Fallback: 'before' timestamp parameter for older records

    The fallback includes infinite loop detection to handle API edge cases
    where the same timestamp appears repeatedly.

    Parameters
    ----------
    auth:
        Required KrakenFuturesAuth instance for authentication.
    start_timestamp:
        Optional start timestamp (ISO 8601 or milliseconds) to seed first load.
        Ignored if state already exists.
    page_size:
        Number of records per API request (default: 500).
    client:
        Optional pre-configured KrakenFuturesClient for testing.

    Yields
    ------
    Mapping[str, Any]:
        Account log records with cursor metadata (_cursor_timestamp_ms, raw_data).

    Raises
    ------
    ValueError:
        If auth is not provided (required for private endpoint).
    """
    if not auth:
        raise ValueError("account_log resource requires authentication")

    state = dlt.current.resource_state()
    client = client or KrakenFuturesClient(auth=auth)

    since = initial_since(start_timestamp, state)
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
        params = prepare_params(params, since, token)

        payload = client.get("/api/history/v2/account-log", params=params, private=True)
        logs = payload.get("logs") or payload.get("accountLog") or []
        next_token = payload.get("continuationToken") or payload.get("continuation_token")

        if not logs:
            state["continuation_token"] = None
            state["before"] = None
            if since:
                state["last_timestamp"] = since
            LOGGER.info("account_log: No more records returned by API")
            log_resource_stats("account_log", records_emitted, state.get("last_timestamp"))
            break

        timestamps: list[Tuple[int, Mapping[str, Any]]] = []
        for log in logs:
            timestamp_ms = extract_timestamp(log, ACCOUNT_LOG_TIMESTAMP_FIELDS)
            if timestamp_ms is None:
                continue
            timestamps.append((timestamp_ms, log))
            if not max_timestamp_seen or timestamp_ms > max_timestamp_seen:
                max_timestamp_seen = timestamp_ms
            record = enrich_record(log, timestamp_ms)
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
                    ms_to_iso(since),
                    ms_to_iso(earliest),
                )
                state["before"] = None
                log_resource_stats("account_log", records_emitted, state.get("last_timestamp"))
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
                        ms_to_iso(earliest),
                        max_before_history,
                    )
                    state["before"] = None
                    log_resource_stats("account_log", records_emitted, state.get("last_timestamp"))
                    break

                before = earliest
                state["before"] = str(before)
                token = None
                LOGGER.debug(
                    "account_log: Using fallback pagination, before=%s, records_so_far=%d",
                    ms_to_iso(earliest),
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
                ms_to_iso(earliest_in_page),
            )
        else:
            LOGGER.info(
                "account_log: Pagination complete - last page was partial (%d < %d records)",
                len(logs),
                page_size,
            )

        state["before"] = None
        log_resource_stats("account_log", records_emitted, state.get("last_timestamp"))
        break


__all__ = ["account_log"]
