"""Executions resource - incremental sync of trade executions."""

from typing import Any, Iterator, Mapping, Optional

import dlt

from ..client import KrakenFuturesClient
from ..auth import KrakenFuturesAuth
from ..helpers import (
    DEFAULT_PAGE_SIZE,
    EXECUTION_TIMESTAMP_FIELDS,
    extract_timestamp,
    initial_since,
    prepare_params,
    log_resource_stats,
    enrich_record,
)


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
    """Load trade execution history with incremental cursor.

    This resource fetches historical trade executions from the Kraken Futures API
    using continuation token-based pagination. State is tracked using the maximum
    timestamp seen in each batch.

    Parameters
    ----------
    auth:
        Optional KrakenFuturesAuth instance for authentication.
        Executions can be accessed without auth for some accounts.
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
        Execution records with cursor metadata (_cursor_timestamp_ms, raw_data).
    """
    state = dlt.current.resource_state()
    client = client or KrakenFuturesClient(auth=auth)

    since = initial_since(start_timestamp, state)
    token = state.get("continuation_token")

    max_timestamp_seen = since

    records_emitted = 0

    while True:
        params = prepare_params({"count": page_size}, since, token)
        payload = client.get("/api/history/v2/executions", params=params, private=True)

        elements = payload.get("elements") or payload.get("executionEvents") or []
        next_token = payload.get("continuationToken") or payload.get("continuation_token")

        if not elements:
            state["continuation_token"] = None
            if since:
                state["last_timestamp"] = since
            log_resource_stats("executions", records_emitted, state.get("last_timestamp"))
            break

        for element in elements:
            timestamp_ms = extract_timestamp(element, EXECUTION_TIMESTAMP_FIELDS)
            if timestamp_ms is None:
                continue
            if not max_timestamp_seen or timestamp_ms > max_timestamp_seen:
                max_timestamp_seen = timestamp_ms
            record = enrich_record(element, timestamp_ms)
            yield record
            records_emitted += 1

        if next_token:
            token = next_token
            state["continuation_token"] = next_token
            continue

        if max_timestamp_seen:
            state["last_timestamp"] = str(max_timestamp_seen)
        state["continuation_token"] = None
        log_resource_stats("executions", records_emitted, state.get("last_timestamp"))
        break


__all__ = ["executions"]
