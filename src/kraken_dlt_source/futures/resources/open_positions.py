"""Open positions resource - snapshot of current account positions."""

from typing import Any, Iterable, Mapping, Optional

import dlt

from ..client import KrakenFuturesClient
from ..auth import KrakenFuturesAuth
from ..helpers import log_resource_stats, json_dumps


@dlt.resource(name="open_positions", write_disposition="replace")
def open_positions(
    auth: Optional[KrakenFuturesAuth],
    client: Optional[KrakenFuturesClient] = None,
) -> Iterable[Mapping[str, Any]]:
    """Load current open positions (snapshot/replace).

    This resource fetches all currently open positions for the authenticated account.
    Data is replaced on each run (no incremental state).

    Parameters
    ----------
    auth:
        Required KrakenFuturesAuth instance for authentication.
    client:
        Optional pre-configured KrakenFuturesClient for testing.

    Yields
    ------
    Mapping[str, Any]:
        Position records with raw_data field.

    Raises
    ------
    ValueError:
        If auth is not provided (required for private endpoint).
    """
    if not auth:
        raise ValueError("open_positions resource requires authentication")

    client = client or KrakenFuturesClient(auth=auth)
    payload = client.get("/derivatives/api/v3/openpositions", params=None, private=True)
    positions = payload.get("openPositions") or payload.get("openpositions") or []

    count = 0
    for position in positions:
        record = dict(position)
        record["raw_data"] = json_dumps(position)
        yield record
        count += 1

    log_resource_stats("open_positions", count, None)


__all__ = ["open_positions"]
