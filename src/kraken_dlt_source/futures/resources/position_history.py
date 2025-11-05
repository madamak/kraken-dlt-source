"""Position history resource - incremental sync with event flattening."""

from typing import Any, Iterator, Mapping, Optional

import dlt

from ..client import KrakenFuturesClient
from ..auth import KrakenFuturesAuth
from ..helpers import (
    DEFAULT_PAGE_SIZE,
    POSITION_HISTORY_TIMESTAMP_FIELDS,
    extract_timestamp,
    initial_since,
    prepare_params,
    log_resource_stats,
    ms_to_iso,
    json_dumps,
)


@dlt.resource(name="position_history", primary_key=["executionUid", "uid"], write_disposition="append")
def position_history(
    auth: Optional[KrakenFuturesAuth],
    start_timestamp: Optional[str] = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    client: Optional[KrakenFuturesClient] = None,
) -> Iterator[Mapping[str, Any]]:
    """Load position update history with nested event flattening.

    This resource extracts PositionUpdate events from nested structures and
    generates synthetic executionUid for funding events (which lack natural IDs).

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
        Position history records with flattened events and cursor metadata.

    Raises
    ------
    ValueError:
        If auth is not provided (required for private endpoint).
    """
    if not auth:
        raise ValueError("position_history resource requires authentication")

    state = dlt.current.resource_state()
    client = client or KrakenFuturesClient(auth=auth)

    since = initial_since(start_timestamp, state)
    token = state.get("continuation_token")

    max_timestamp_seen = since

    records_emitted = 0

    while True:
        params = prepare_params({"count": page_size}, since, token)
        payload = client.get("/api/history/v3/positions", params=params, private=True)

        elements = payload.get("elements") or []
        next_token = payload.get("continuationToken") or payload.get("continuation_token")

        if not elements:
            state["continuation_token"] = None
            if since:
                state["last_timestamp"] = since
            log_resource_stats("position_history", records_emitted, state.get("last_timestamp"))
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

            timestamp_ms = extract_timestamp(normalized, POSITION_HISTORY_TIMESTAMP_FIELDS)
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
            record["_cursor_timestamp"] = ms_to_iso(timestamp_ms)
            record["raw_data"] = json_dumps(normalized)
            yield record
            records_emitted += 1

        if next_token:
            token = next_token
            state["continuation_token"] = next_token
            continue

        if max_timestamp_seen:
            state["last_timestamp"] = str(max_timestamp_seen)
        state["continuation_token"] = None
        log_resource_stats("position_history", records_emitted, state.get("last_timestamp"))
        break


__all__ = ["position_history"]
