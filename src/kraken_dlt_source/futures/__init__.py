from typing import Iterable, Optional, Sequence

import dlt

from .auth import KrakenFuturesAuth
from . import resources


@dlt.source(name="kraken_futures")
def kraken_futures_source(
    api_key: Optional[str] = dlt.secrets.value,
    api_secret: Optional[str] = dlt.secrets.value,
    start_timestamp: Optional[str] = None,
    resources_to_load: Optional[Sequence[str]] = None,
    auth: Optional[KrakenFuturesAuth] = None,
    page_size: Optional[int] = None,
) -> Iterable:
    """Assemble Kraken Futures REST resources for DLT.

    By default, dlt will look for credentials in environment variables or secrets.toml:
    - KRAKEN_FUTURES__API_KEY or kraken_futures.api_key
    - KRAKEN_FUTURES__API_SECRET or kraken_futures.api_secret

    Parameters
    ----------
    api_key:
        Kraken Futures public API key. Required for private endpoints.
        Defaults to KRAKEN_FUTURES__API_KEY from environment or secrets.
    api_secret:
        Kraken Futures secret API key (base64 encoded). Required for private endpoints.
        Defaults to KRAKEN_FUTURES__API_SECRET from environment or secrets.
    start_timestamp:
        Optional millisecond timestamp (or ISO8601 string) to seed initial
        incremental imports.
    resources_to_load:
        Explicit subset of resource names to include. If omitted, all resources
        are wired into the source.
    auth:
        Optional pre-configured KrakenFuturesAuth instance. If provided, api_key
        and api_secret parameters are ignored.
    page_size:
        Number of records to fetch per API request. Defaults to 500.
        For historical backfills, consider 5000 for optimal rate limit efficiency.
        Maximum values: account_log=100000, executions/positions=5000+ (untested).
    """

    if auth is None and api_key and api_secret:
        auth = KrakenFuturesAuth(api_key, api_secret)

    selected = set(resources_to_load or resources.ALL_RESOURCE_NAMES)

    if "executions" in selected:
        yield resources.executions(auth=auth, start_timestamp=start_timestamp, page_size=page_size or resources.DEFAULT_PAGE_SIZE)

    if "account_log" in selected:
        yield resources.account_log(auth=auth, start_timestamp=start_timestamp, page_size=page_size or resources.DEFAULT_PAGE_SIZE)

    if "position_history" in selected:
        yield resources.position_history(auth=auth, start_timestamp=start_timestamp, page_size=page_size or resources.DEFAULT_PAGE_SIZE)

    if "tickers" in selected:
        yield resources.tickers()

    if "open_positions" in selected:
        yield resources.open_positions(auth=auth)


__all__ = ["kraken_futures_source", "KrakenFuturesAuth"]
