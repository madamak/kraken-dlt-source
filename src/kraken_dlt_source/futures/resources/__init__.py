"""Kraken Futures DLT resources - modular resource definitions.

This package contains individual resource implementations for different Kraken Futures
data endpoints. Each resource is defined in its own module for better maintainability.

Available Resources
-------------------
- executions: Incremental trade execution history
- account_log: Incremental account activity log with hybrid pagination
- position_history: Incremental position update history with event flattening
- tickers: Snapshot of current market ticker data (public)
- open_positions: Snapshot of current open positions (private)
"""

from .executions import executions
from .account_log import account_log
from .position_history import position_history
from .tickers import tickers
from .open_positions import open_positions

# Tuple of all available resource names for validation and listing
ALL_RESOURCE_NAMES = ("executions", "account_log", "position_history", "tickers", "open_positions")

__all__ = [
    "executions",
    "account_log",
    "position_history",
    "tickers",
    "open_positions",
    "ALL_RESOURCE_NAMES",
]
