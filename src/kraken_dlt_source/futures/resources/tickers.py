"""Tickers resource - snapshot of current market ticker data."""

from typing import Any, Iterable, Mapping, Optional

import dlt

from ..client import KrakenFuturesClient
from ..helpers import log_resource_stats, json_dumps


@dlt.resource(name="tickers", write_disposition="replace")
def tickers(client: Optional[KrakenFuturesClient] = None) -> Iterable[Mapping[str, Any]]:
    """Load current ticker data for all instruments (snapshot/replace).

    This is a public endpoint that returns current market data without requiring
    authentication. Data is replaced on each run (no incremental state).

    Parameters
    ----------
    client:
        Optional pre-configured KrakenFuturesClient for testing.

    Yields
    ------
    Mapping[str, Any]:
        Ticker records with raw_data field.
    """
    client = client or KrakenFuturesClient()
    payload = client.get("/derivatives/api/v3/tickers", params=None, private=False)
    tickers_data = payload.get("tickers") or []

    count = 0
    for ticker in tickers_data:
        record = dict(ticker)
        record["raw_data"] = json_dumps(ticker)
        yield record
        count += 1

    log_resource_stats("tickers", count, None)


__all__ = ["tickers"]
