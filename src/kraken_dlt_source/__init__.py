"""Kraken DLT Source - Unified connector for Kraken Futures and Spot APIs.

This package provides dlt sources for ingesting data from Kraken's various APIs:
- Futures: Trading history, account logs, positions, tickers
- Spot: (Coming soon) Balances, trades, orders

Example:
    >>> from kraken_dlt_source.futures import kraken_futures_source
    >>> import dlt
    >>>
    >>> pipeline = dlt.pipeline(
    ...     pipeline_name="kraken_futures",
    ...     destination="duckdb",
    ...     dataset_name="kraken_data"
    ... )
    >>>
    >>> info = pipeline.run(kraken_futures_source())
"""

# Re-export Futures source for convenience
from kraken_dlt_source.futures import kraken_futures_source

__version__ = "0.1.0"
__all__ = ["kraken_futures_source"]
