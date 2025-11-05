# DuckDB Database Structure - Kraken Futures Data

## Overview

This document describes the DuckDB database structure for Kraken Futures trading data. The database is located at `./kraken_futures.duckdb` and contains trading executions, position history, account logs, open positions, and market ticker data.

**Database Location**: `./kraken_futures.duckdb`

## Schemas

The database contains multiple schemas:
- `kraken_futures_data` - Primary data schema containing all trading and market data
- `kraken_futures_data_YYYYMMDDHHMMSS` - Timestamped backup/versioned schemas
- `main`, `information_schema`, `pg_catalog` - Standard system schemas

**Primary Schema**: `kraken_futures_data`

## Data Coverage

### Current Data Statistics
- **Executions**: 4,004 records (2025-02-25 to 2025-11-05)
- **Position History**: 23,951 records (2025-02-25 to 2025-11-05)
- **Account Log**: 26,099 records (2025-09-09 to 2025-11-05)
- **Open Positions**: 6 current positions
- **Tickers**: Market data snapshots

### Traded Symbols
The following Kraken perpetual futures contracts are present in the data:
- `PF_ALGOUSD` - Algorand/USD
- `PF_ETHUSD` - Ethereum/USD
- `PF_ORDIUSD` - Ordinals/USD
- `PF_SOLUSD` - Solana/USD
- `PF_STXUSD` - Stacks/USD
- `PF_SUIUSD` - Sui/USD
- `PF_WIFUSD` - dogwifhat/USD
- `PF_XBTUSD` - Bitcoin/USD
- `PF_XRPUSD` - Ripple/USD

## Table Structures

### 1. executions

**Description**: Complete record of all trade executions from Kraken Futures API.

**Schema**: `kraken_futures_data.executions`

**Key Columns**:
- `uid` (VARCHAR, NOT NULL) - Unique execution event identifier
- `timestamp` (BIGINT) - Unix timestamp in milliseconds
- `_cursor_timestamp` (TIMESTAMP WITH TIME ZONE) - Converted timestamp for querying
- `_cursor_timestamp_ms` (BIGINT) - Cursor position for incremental loading

**Order Information** (nested fields prefixed with `event__execution__execution__order__`):
- `tradeable` (VARCHAR) - Contract symbol (e.g., PF_ETHUSD)
- `direction` (VARCHAR) - "Buy" or "Sell"
- `quantity` (VARCHAR) - Order size
- `filled` (VARCHAR) - Amount filled
- `order_type` (VARCHAR) - Order type (e.g., "IoC", "Limit")
- `limit_price` (VARCHAR) - Limit price if applicable
- `reduce_only` (BOOLEAN) - Whether this is a position-reducing order
- `account_uid` (VARCHAR) - Account identifier
- `client_id` (VARCHAR) - User-provided order ID

**Execution Details** (nested fields prefixed with `event__execution__execution__`):
- `uid` (VARCHAR) - Execution identifier
- `timestamp` (BIGINT) - Execution time in milliseconds
- `price` (VARCHAR) - Execution price
- `quantity` (VARCHAR) - Executed quantity
- `mark_price` (VARCHAR) - Mark price at execution
- `execution_type` (VARCHAR) - "taker" or "maker"
- `usd_value` (VARCHAR) - USD notional value
- `limit_filled` (BOOLEAN) - Whether limit order was filled

**Fee and Position Data** (nested fields):
- `event__execution__execution__order_data__fee` (VARCHAR) - Fee amount
- `event__execution__execution__order_data__fee_calculation_info__percentage_fee` (VARCHAR) - Fee percentage
- `event__execution__execution__order_data__position_size` (VARCHAR) - Position size after execution
- `event__execution__execution__regulatory_data__venue` (VARCHAR) - Trading venue (e.g., "CFL")

**DLT Metadata**:
- `_dlt_load_id` (VARCHAR, NOT NULL) - Load batch identifier
- `_dlt_id` (VARCHAR, NOT NULL) - Unique row identifier
- `raw_data` (VARCHAR) - Original JSON from API

**Record Count**: 4,004 records

**Date Range**: 2025-02-25 13:07:15 to 2025-11-05 06:14:04

**Sample Query**:
```sql
SELECT
    uid,
    _cursor_timestamp,
    event__execution__execution__order__tradeable as symbol,
    event__execution__execution__order__direction as direction,
    event__execution__execution__price as price,
    event__execution__execution__quantity as quantity,
    event__execution__execution__execution_type as exec_type,
    event__execution__execution__usd_value as usd_value
FROM kraken_futures_data.executions
ORDER BY _cursor_timestamp DESC
LIMIT 10;
```

---

### 2. position_history

**Description**: Historical record of all position changes, including trades, funding events, and position updates.

**Schema**: `kraken_futures_data.position_history`

**Key Columns**:
- `uid` (VARCHAR, NOT NULL) - Position update identifier
- `execution_uid` (VARCHAR, NOT NULL) - Related execution ID
- `timestamp` (BIGINT) - Unix timestamp in milliseconds
- `_cursor_timestamp` (TIMESTAMP WITH TIME ZONE) - Converted timestamp
- `account_uid` (VARCHAR) - Account identifier
- `tradeable` (VARCHAR) - Contract symbol

**Position State**:
- `old_position` (VARCHAR) - Position size before update
- `new_position` (VARCHAR) - Position size after update
- `old_average_entry_price` (VARCHAR) - Previous entry price
- `new_average_entry_price` (VARCHAR) - Updated entry price
- `position_change` (VARCHAR) - Type of change (e.g., "decrease", "increase")

**Trade Details**:
- `fill_time` (BIGINT) - Trade fill timestamp
- `execution_price` (VARCHAR) - Execution price
- `execution_size` (VARCHAR) - Size of execution
- `trade_type` (VARCHAR) - "userExecution" or other types

**Financial Impact**:
- `realized_pn_l` (VARCHAR) - Realized profit/loss
- `fee` (VARCHAR) - Fee amount
- `fee_currency` (VARCHAR) - Currency of fee (usually "USD")
- `realized_funding` (VARCHAR) - Realized funding payment
- `funding_realization_time` (BIGINT) - Funding timestamp

**Metadata**:
- `update_reason` (VARCHAR) - Reason for update (e.g., "trade")
- `_dlt_load_id` (VARCHAR, NOT NULL) - Load batch identifier
- `_dlt_id` (VARCHAR, NOT NULL) - Unique row identifier
- `raw_data` (VARCHAR) - Original JSON from API

**Record Count**: 23,951 records

**Date Range**: 2025-02-25 13:07:15 to 2025-11-05 11:00:00

**Sample Query**:
```sql
SELECT
    uid,
    _cursor_timestamp,
    tradeable,
    old_position,
    new_position,
    position_change,
    realized_pn_l,
    execution_price,
    fee
FROM kraken_futures_data.position_history
WHERE update_reason = 'trade'
ORDER BY _cursor_timestamp DESC
LIMIT 10;
```

---

### 3. account_log

**Description**: Complete account activity log including trades, funding payments, interest, conversions, and transfers.

**Schema**: `kraken_futures_data.account_log`

**Key Columns**:
- `booking_uid` (VARCHAR, NOT NULL) - Unique booking identifier
- `id` (BIGINT) - Sequential log entry ID
- `date` (TIMESTAMP WITH TIME ZONE) - Event timestamp
- `_cursor_timestamp` (TIMESTAMP WITH TIME ZONE) - Loading cursor timestamp
- `info` (VARCHAR) - Event type description

**Event Types** (values in `info` column):
- `futures trade` - Trading activity
- `funding rate change` - Funding rate payments
- `interest payment` - Interest charges/credits
- `conversion` - Currency conversions
- `cross-exchange transfer` - Transfers between exchanges

**Account Details**:
- `margin_account` (VARCHAR) - Account type (e.g., "flex")
- `asset` (VARCHAR) - Asset/currency (e.g., "usd", "btc")
- `execution` (VARCHAR) - Related execution UID

**Balance Changes**:
- `old_balance` (DOUBLE) - Balance before event
- `new_balance` (DOUBLE) - Balance after event
- `fee` (DOUBLE) - Fee amount (negative values indicate charges)

**Trade-Specific Fields** (populated when `info = 'futures trade'`):
- `contract` (VARCHAR) - Contract symbol
- `trade_price` (DOUBLE) - Trade execution price
- `realized_pnl` (DOUBLE) - Realized profit/loss
- `old_average_entry_price` (DOUBLE) - Entry price before trade
- `new_average_entry_price` (DOUBLE) - Entry price after trade
- `position_uid` (VARCHAR) - Position identifier

**Funding-Specific Fields** (populated when `info = 'funding rate change'`):
- `funding_rate` (DOUBLE) - Applied funding rate
- `realized_funding` (DOUBLE) - Funding payment amount
- `mark_price` (DOUBLE) - Mark price at funding time

**Other Fields**:
- `conversion_spread_percentage` (DOUBLE) - Conversion fee percentage
- `collateral` (DOUBLE) - Collateral amount
- `liquidation_fee` (DOUBLE) - Liquidation fees if applicable

**DLT Metadata**:
- `_dlt_load_id` (VARCHAR, NOT NULL) - Load batch identifier
- `_dlt_id` (VARCHAR, NOT NULL) - Unique row identifier
- `raw_data` (VARCHAR) - Original JSON from API

**Record Count**: 26,099 records

**Date Range**: 2025-09-09 09:00:00 to 2025-11-05 11:16:53

**Sample Queries**:

**Recent account activity**:
```sql
SELECT
    date,
    info,
    asset,
    old_balance,
    new_balance,
    fee,
    contract,
    realized_pnl
FROM kraken_futures_data.account_log
ORDER BY date DESC
LIMIT 20;
```

**Funding payments summary**:
```sql
SELECT
    contract,
    COUNT(*) as funding_events,
    SUM(realized_funding) as total_funding,
    AVG(funding_rate) as avg_funding_rate
FROM kraken_futures_data.account_log
WHERE info = 'funding rate change'
GROUP BY contract
ORDER BY total_funding DESC;
```

**Trading fees by contract**:
```sql
SELECT
    contract,
    COUNT(*) as trade_count,
    SUM(ABS(fee)) as total_fees,
    AVG(ABS(fee)) as avg_fee_per_trade
FROM kraken_futures_data.account_log
WHERE info = 'futures trade'
GROUP BY contract
ORDER BY total_fees DESC;
```

---

### 4. open_positions

**Description**: Current snapshot of all open trading positions.

**Schema**: `kraken_futures_data.open_positions`

**Key Columns**:
- `symbol` (VARCHAR) - Contract symbol (e.g., PF_ETHUSD)
- `side` (VARCHAR) - Position direction ("long" or "short")
- `size` (BIGINT) - Position size in contracts
- `size__v_double` (DOUBLE) - Position size as double (for fractional positions)
- `price` (DOUBLE) - Average entry price
- `fill_time` (TIMESTAMP WITH TIME ZONE) - Time position was opened/last modified
- `unrealized_funding` (DOUBLE) - Accumulated unrealized funding
- `pnl_currency` (VARCHAR) - Currency for P&L calculation (BTC, ETH, USD, SOL, etc.)

**DLT Metadata**:
- `_dlt_load_id` (VARCHAR, NOT NULL) - Load batch identifier
- `_dlt_id` (VARCHAR, NOT NULL) - Unique row identifier
- `raw_data` (VARCHAR) - Original JSON from API

**Record Count**: 6 positions (as of latest load)

**Current Positions** (as of 2025-11-05 11:49:23):
1. **PF_XRPUSD**: 11,250 contracts long @ 2.85 (BTC-denominated)
2. **PF_ETHUSD**: 43.5 contracts long @ 4,294.33 (ETH-denominated)
3. **PF_ORDIUSD**: 2,900 contracts long @ 8.93 (USD-denominated)
4. **PF_SOLUSD**: 250 contracts long @ 212.65 (SOL-denominated)
5. **PF_XBTUSD**: 2.225 contracts long @ 116,578.83 (BTC-denominated)
6. **PF_SUIUSD**: 13,400 contracts long @ 3.33 (BTC-denominated)

**Sample Query**:
```sql
SELECT
    symbol,
    side,
    COALESCE(size__v_double, size) as position_size,
    price as entry_price,
    unrealized_funding,
    pnl_currency,
    fill_time
FROM kraken_futures_data.open_positions
ORDER BY symbol;
```

---

### 5. tickers

**Description**: Market ticker data snapshots including prices, volumes, funding rates, and market statistics.

**Schema**: `kraken_futures_data.tickers`

**Key Columns**:
- `symbol` (VARCHAR) - Contract symbol
- `tag` (VARCHAR) - Ticker tag/category
- `pair` (VARCHAR) - Trading pair identifier

**Price Data**:
- `last` (DOUBLE) - Last traded price
- `last_time` (TIMESTAMP WITH TIME ZONE) - Time of last trade
- `last_size` (DOUBLE) - Size of last trade
- `bid` (DOUBLE) - Best bid price
- `bid_size` (DOUBLE) - Size at best bid
- `ask` (DOUBLE) - Best ask price
- `ask_size` (DOUBLE) - Size at best ask
- `mark_price` (DOUBLE) - Current mark price
- `index_price` (DOUBLE) - Underlying index price

**24-Hour Statistics**:
- `open24h` (DOUBLE) - Opening price 24h ago
- `high24h` (DOUBLE) - 24-hour high
- `low24h` (DOUBLE) - 24-hour low
- `change24h` (DOUBLE) - 24-hour price change
- `vol24h` (DOUBLE) - 24-hour volume in contracts
- `volume_quote` (DOUBLE) - 24-hour volume in quote currency

**Market Data**:
- `open_interest` (DOUBLE) - Total open interest
- `funding_rate` (DOUBLE) - Current funding rate
- `funding_rate_prediction` (DOUBLE) - Predicted next funding rate

**Market Status**:
- `suspended` (BOOLEAN) - Whether trading is suspended
- `post_only` (BOOLEAN) - Post-only mode status
- `is_underlying_market_closed` (BOOLEAN) - Underlying market status

**DLT Metadata**:
- `_dlt_load_id` (VARCHAR, NOT NULL) - Load batch identifier
- `_dlt_id` (VARCHAR, NOT NULL) - Unique row identifier
- `raw_data` (VARCHAR) - Original JSON from API

**Sample Query**:
```sql
SELECT
    symbol,
    last as price,
    mark_price,
    bid,
    ask,
    vol24h,
    open_interest,
    funding_rate,
    change24h,
    last_time
FROM kraken_futures_data.tickers
WHERE suspended = false
ORDER BY vol24h DESC;
```

---

### 6. DLT Metadata Tables

#### _dlt_loads

**Description**: Tracks all data load operations performed by dlt pipeline.

**Schema**: `kraken_futures_data._dlt_loads`

**Columns**:
- `load_id` (VARCHAR) - Unique load identifier (Unix timestamp with decimals)
- `schema_name` (VARCHAR) - Schema being loaded (e.g., "kraken_futures")
- `status` (BIGINT) - Load status (0 = success)
- `inserted_at` (TIMESTAMP WITH TIME ZONE) - When load was recorded
- `schema_version_hash` (VARCHAR) - Hash of schema version

**Sample Query**:
```sql
SELECT
    load_id,
    schema_name,
    status,
    inserted_at
FROM kraken_futures_data._dlt_loads
ORDER BY inserted_at DESC
LIMIT 10;
```

#### _dlt_version

**Description**: Schema version tracking for dlt.

**Columns**:
- `version` (BIGINT) - dlt schema version
- `engine_version` (BIGINT) - dlt engine version
- `inserted_at` (TIMESTAMP WITH TIME ZONE) - Version timestamp
- `schema_name` (VARCHAR) - Schema name
- `version_hash` (VARCHAR) - Version hash
- `schema` (VARCHAR) - Full schema definition (JSON)

#### _dlt_pipeline_state

**Description**: Pipeline state and configuration persistence.

**Columns**:
- `version` (BIGINT) - State version
- `engine_version` (BIGINT) - Engine version
- `pipeline_name` (VARCHAR) - Pipeline identifier
- `state` (VARCHAR) - Serialized state data
- `created_at` (TIMESTAMP WITH TIME ZONE) - State creation time
- `version_hash` (VARCHAR) - State hash
- `_dlt_load_id` (VARCHAR, NOT NULL) - Load batch identifier
- `_dlt_id` (VARCHAR, NOT NULL) - Unique row identifier

---

## Common Query Patterns

### Calculate P&L for a Specific Symbol

```sql
SELECT
    tradeable,
    SUM(CAST(realized_pn_l AS DOUBLE)) as total_realized_pnl,
    SUM(CAST(fee AS DOUBLE)) as total_fees,
    COUNT(*) as num_trades
FROM kraken_futures_data.position_history
WHERE tradeable = 'PF_ETHUSD'
    AND update_reason = 'trade'
    AND realized_pn_l IS NOT NULL
GROUP BY tradeable;
```

### Daily Trading Volume by Symbol

```sql
SELECT
    event__execution__execution__order__tradeable as symbol,
    DATE_TRUNC('day', _cursor_timestamp) as trade_date,
    COUNT(*) as execution_count,
    SUM(CAST(event__execution__execution__quantity AS DOUBLE)) as total_quantity,
    SUM(CAST(event__execution__execution__usd_value AS DOUBLE)) as total_usd_volume
FROM kraken_futures_data.executions
WHERE event__execution__execution__order__tradeable IS NOT NULL
GROUP BY symbol, trade_date
ORDER BY trade_date DESC, total_usd_volume DESC;
```

### Funding Payments Analysis

```sql
SELECT
    contract,
    DATE_TRUNC('day', date) as funding_date,
    COUNT(*) as funding_events,
    SUM(realized_funding) as total_funding,
    AVG(funding_rate) as avg_funding_rate,
    AVG(mark_price) as avg_mark_price
FROM kraken_futures_data.account_log
WHERE info = 'funding rate change'
GROUP BY contract, funding_date
ORDER BY funding_date DESC, contract;
```

### Join Executions with Position History

```sql
SELECT
    e.uid as execution_id,
    e._cursor_timestamp as execution_time,
    e.event__execution__execution__order__tradeable as symbol,
    e.event__execution__execution__price as price,
    e.event__execution__execution__quantity as quantity,
    e.event__execution__execution__execution_type as exec_type,
    p.realized_pn_l,
    p.fee,
    p.new_position,
    p.new_average_entry_price
FROM kraken_futures_data.executions e
INNER JOIN kraken_futures_data.position_history p
    ON e.event__execution__execution__uid = p.execution_uid
WHERE e.event__execution__execution__order__tradeable = 'PF_XBTUSD'
ORDER BY e._cursor_timestamp DESC
LIMIT 20;
```

### Account Balance Evolution

```sql
SELECT
    date,
    info,
    asset,
    old_balance,
    new_balance,
    (new_balance - old_balance) as balance_change,
    fee,
    execution
FROM kraken_futures_data.account_log
WHERE asset = 'usd'
ORDER BY date DESC
LIMIT 50;
```

### Trading Performance by Time of Day

```sql
SELECT
    EXTRACT(HOUR FROM _cursor_timestamp) as hour_of_day,
    event__execution__execution__order__tradeable as symbol,
    COUNT(*) as trade_count,
    AVG(CAST(event__execution__execution__quantity AS DOUBLE)) as avg_quantity,
    SUM(CAST(event__execution__execution__usd_value AS DOUBLE)) as total_volume
FROM kraken_futures_data.executions
WHERE event__execution__execution__order__tradeable IS NOT NULL
GROUP BY hour_of_day, symbol
ORDER BY hour_of_day, symbol;
```

---

## Important Notes for AI Agents

### Data Type Considerations

1. **Timestamps**: The database uses multiple timestamp formats:
   - `timestamp` fields: Unix timestamps in **milliseconds** (BIGINT)
   - `_cursor_timestamp` fields: Properly typed TIMESTAMP WITH TIME ZONE
   - Always use `_cursor_timestamp` for date filtering and display

2. **Numeric Strings**: Many numeric fields are stored as VARCHAR:
   - `quantity`, `price`, `realized_pn_l`, `fee`, etc.
   - Must use `CAST(field AS DOUBLE)` for mathematical operations
   - This preserves precision from the API responses

3. **Nested Field Naming**: Execution data uses double underscores:
   - Format: `event__execution__execution__field__subfield`
   - Example: `event__execution__execution__order__tradeable`

### Query Performance Tips

1. **Use indexes on cursor timestamps** for date range queries
2. **Filter early** using WHERE clauses before JOINs
3. **Use `_dlt_load_id`** to query specific data loads
4. **Leverage `raw_data`** column when you need original API response

### Data Integrity

1. **Primary Keys**:
   - `executions`: `uid`
   - `position_history`: `uid` + `execution_uid`
   - `account_log`: `booking_uid`
   - All tables: `_dlt_id` (unique per row)

2. **Foreign Key Relationships**:
   - `executions.event__execution__execution__uid` ↔ `position_history.execution_uid`
   - `executions.uid` ↔ `account_log.execution`

3. **NULL Values**: Most fields are nullable; always check for NULL in aggregations

### Incremental Loading

The pipeline uses cursor-based incremental loading:
- `_cursor_timestamp_ms`: Tracks the last processed timestamp
- `_dlt_load_id`: Identifies each pipeline run
- Data is append-only; no updates or deletes

### Raw Data Access

Every table includes a `raw_data` VARCHAR column containing the original JSON response from Kraken's API. This is useful for:
- Debugging data transformation issues
- Accessing fields not yet extracted to columns
- Verifying data integrity

**Example**:
```sql
SELECT raw_data::JSON->>'symbol' as symbol
FROM kraken_futures_data.tickers
LIMIT 1;
```

---

## CLI Access

To access the database from command line:

```bash
# Interactive shell
duckdb kraken_futures.duckdb

# Execute single query
duckdb kraken_futures.duckdb -c "SELECT COUNT(*) FROM kraken_futures_data.executions;"

# Export to CSV
duckdb kraken_futures.duckdb -c "COPY (SELECT * FROM kraken_futures_data.open_positions) TO 'positions.csv' WITH (HEADER, DELIMITER ',');"

# With uv (recommended in this project)
uv run duckdb kraken_futures.duckdb
```

---

## Schema Evolution

The database supports schema evolution through dlt:
- New columns are added automatically when API responses include new fields
- Schema versions are tracked in `_dlt_version` table
- Old schemas may be preserved with timestamp suffixes

---

## Contact & Support

For issues or questions about this database structure:
- Check pipeline logs in the dlt execution output
- Review `_dlt_loads` table for load failures (status != 0)
- Examine `raw_data` columns to understand source data format
