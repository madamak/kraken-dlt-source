# Kraken Futures API Reference

This document provides quick links to the official Kraken Futures API documentation for endpoints used in this DLT source.

## Base URLs
- **Futures API**: `https://futures.kraken.com`
- **Derivatives API**: `https://futures.kraken.com/derivatives`

## Official Documentation
- [Kraken Futures API Overview](https://docs.futures.kraken.com/)
- [Kraken API Center - Futures Section](https://docs.kraken.com/api/docs/futures-api)
- [Support: Derivatives API Documentation](https://support.kraken.com/hc/en-us/articles/8832693094036-Futures-API-Documentation)

## Implemented Endpoints

### History Endpoints (Incremental)

#### Executions
- **Endpoint**: `GET /api/history/v2/executions`
- **Documentation**: [Get executions](https://docs.kraken.com/api/docs/futures-api/history/executions/)
- **Resource**: `executions` in [resources.py](../src/kraken_dlt_source/futures/resources.py)
- **Authentication**: Required (private endpoint)
- **Pagination**: Continuation token + timestamp (`since`, `before`)
- **Primary Key**: `uid`
- **Parameters**:
  - `since`: Start timestamp (milliseconds)
  - `before`: End timestamp (milliseconds)
  - `count`: Page size (default: 500)
  - `continuation_token`: Pagination token from previous response
  - `tradeable`: Optional symbol filter

#### Account Log
- **Endpoint**: `GET /api/history/v2/account-log`
- **Documentation**: [Get account log](https://docs.kraken.com/api/docs/futures-api/history/account-log/)
- **Resource**: `account_log` in [resources.py](../src/kraken_dlt_source/futures/resources.py)
- **Authentication**: Required (private endpoint)
- **Pagination**: Continuation token + timestamp with fallback to `before` paging
- **Primary Key**: `booking_uid`
- **Parameters**:
  - `since`: Start timestamp (milliseconds)
  - `before`: End timestamp (milliseconds)
  - `count`: Page size (default: 500)
  - `continuation_token`: Pagination token from previous response
  - `type`: Optional event type filter
- **Conditional Fields**: Some fields only appear for specific event types:
  - `collateral`: Present in margin/collateral events
  - `liquidation_fee`: Present in liquidation events
  - `position_uid`: Present in position-related logs

#### Position History
- **Endpoint**: `GET /api/history/v3/positions`
- **Documentation**: [Get position history](https://docs.kraken.com/api/docs/futures-api/history/position-history/)
- **Resource**: `position_history` in [resources.py](../src/kraken_dlt_source/futures/resources.py)
- **Authentication**: Required (private endpoint)
- **Pagination**: Continuation token + timestamp
- **Primary Key**: `["executionUid", "uid"]` (composite)
- **Parameters**:
  - `since`: Start timestamp (milliseconds)
  - `before`: End timestamp (milliseconds)
  - `count`: Page size (default: 500)
  - `continuation_token`: Pagination token from previous response
  - `sort`: Sort order (default: ascending by timestamp)
  - `tradeable`: Optional symbol filter

### Market Data Endpoints (Snapshot)

#### Tickers
- **Endpoint**: `GET /derivatives/api/v3/tickers`
- **Documentation**: [Get tickers](https://docs.kraken.com/api/docs/futures-api/market-data/tickers/)
- **Resource**: `tickers` in [resources.py](../src/kraken_dlt_source/futures/resources.py)
- **Authentication**: Not required (public endpoint)
- **Write Disposition**: Replace (snapshot)
- **Returns**: Current ticker data for all tradeable instruments

#### Open Positions
- **Endpoint**: `GET /derivatives/api/v3/openpositions`
- **Documentation**: [Get open positions](https://docs.kraken.com/api/docs/futures-api/trading/open-positions/)
- **Resource**: `open_positions` in [resources.py](../src/kraken_dlt_source/futures/resources.py)
- **Authentication**: Required (private endpoint)
- **Write Disposition**: Replace (snapshot)
- **Returns**: Current open positions for the authenticated account

## Authentication

All private endpoints require HMAC-SHA512 authentication with the following headers:
- `APIKey`: Your public API key
- `Authent`: HMAC-SHA512 signature (base64 encoded)
- `Nonce`: Timestamp in milliseconds + 5-digit counter

**Implementation**: See [auth.py](../src/kraken_dlt_source/futures/auth.py)

**Documentation**: [Authentication Guide](https://docs.kraken.com/api/docs/futures-api/overview/authentication/)

## Rate Limits

Kraken enforces rate limits on API requests. When rate limited (HTTP 429), the response includes:
- `rate-limit-reset`: Seconds until rate limit resets

**Handling**: Our client implements exponential backoff with jitter (see `KrakenFuturesClient` in resources.py)

## Response Formats

### Successful Response
```json
{
  "result": "success",
  "elements": [...],  // or "logs", "tickers", etc. depending on endpoint
  "continuationToken": "abc123"  // if more pages available
}
```

### Error Response
```json
{
  "result": "error",
  "error": "error message"
}
```

## Additional Resources

- [Kraken API Support](https://support.kraken.com/hc/en-us/sections/360012894412-API)
- [Kraken Futures WebSocket API](https://docs.kraken.com/api/docs/futures-api/websocket/) (not implemented in this source)
- [API Change Log](https://docs.kraken.com/api/docs/change-log/)

## Performance & Optimization

For historical backfills loading large amounts of data, see:
- [Backfill Optimization Guide](./backfill_optimization.md) - How to use `--backfill` mode for 5-60× faster data loading

## Related Documentation

- [Implementation Plan](./plans/000-initial_plan.md) - Detailed technical implementation plan
- [Project Setup](./plans/001_initial_project_setup.md) - Initial project setup notes
- [Backfill Optimization Guide](./backfill_optimization.md) - Optimize for historical data loads
