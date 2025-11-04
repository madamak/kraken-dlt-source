### Kraken Futures DLT Source – Implementation Plan

#### Goal
Build a DLT-based source connector that ingests Kraken Futures REST API data with robust incremental sync, mirroring the semantics proven in `calculator-api` (raw payload preservation, cursor behavior, and continuation token handling).

#### Decisions
- Scope: Futures-only (Spot/Pro out of scope for Phase 1)
- Destination: DuckDB local to start; source code remains destination-agnostic
- Schedule: run incremental sync every 15 minutes
- Secrets: use `KRAKEN_API_PUBLIC_KEY` and `KRAKEN_API_SECRET_KEY` as environment variables [[confirmed]]

#### Scope (Phase 1)
- Executions history (incremental via timestamp + continuation token)
- Account log (incremental via timestamp + continuation token with fallback paging by before)
- Position history events (incremental via continuation token)
- Tickers (snapshot; replace strategy)
- Open positions (snapshot; replace strategy)

Non-goals (Phase 1)
- Websocket streaming
- Spot/Pro endpoints (focus is Futures)

---

### Architecture
- Code-first DLT source using `dlt` and `dlt.sources.helpers.rest_client`:
  - Custom HMAC auth (headers: APIKey, Authent, Nonce) aligned with Kraken Futures auth flow.
  - Resource generators for each endpoint; yield raw objects and add minimal normalization for cursor fields.
- Destination: DuckDB (local), dataset `kraken_futures`. Note: DLT sources are decoupled from destinations; fetching logic and schemas do not depend on the destination. The destination is selected when creating/running the pipeline.
- Secrets: use environment variables `KRAKEN_API_PUBLIC_KEY` and `KRAKEN_API_SECRET_KEY` (preferred) or `.dlt/secrets.toml`.

Directory layout
- `src/kraken_futures_source/`
  - `auth.py` – DLT `BaseAuth` subclass generating Nonce + HMAC signature
  - `resources.py` – resource functions (executions, account_log, position_history, tickers, open_positions)
  - `__init__.py` – `@dlt.source` that wires resources and common config
- `pipelines/run_pipeline.py` – runnable entrypoint to load selected resources
- `documentation/` – this plan and usage docs

---

### Endpoints and Semantics (anchored to calculator-api)
- Base URL: `https://futures.kraken.com`
- Executions: `GET /api/history/v2/executions` with params: `since`, `before`, `tradeable`, `continuation_token`
  - Response fields: items under `elements` or `executionEvents`, next token at `continuationToken`
  - Cursor: prefer `timestamp` (ms) with token; dedupe by `uid`
- Account log: `GET /api/history/v2/account-log` with `since`, `before`, `type`, `count`, `continuation_token`
  - Response fields: `logs` or `accountLog`, token in `continuation_token` or `continuationToken`
  - Cursor: ISO `date`/`timestamp` coalesced; fallback paging by `before` when token is missing
- Position history: `GET /api/history/v3/positions` with `since`, `before`, `sort`, `tradeable`, `continuation_token`
  - Response fields: `elements` list; each has `event` with `PositionUpdate`
  - Cursor: `timestamp` (ms) with token
- Tickers: `GET /derivatives/api/v3/tickers` → `tickers`
- Open positions: `GET /derivatives/api/v3/openpositions` → `openPositions`

Auth
- Headers per request: `APIKey`, `Authent`, `Nonce`
- Signature: `sha256(postData + nonce + endpointPath)` → HMAC-SHA512(secret) → base64
- Nonce: append 5-digit counter to current ms epoch

---

### Incremental Strategy

#### Cursor Tracking
Use DLT's `dlt.current.resource_state()` to persist two pieces of state per resource:
- **`last_timestamp`**: The most recent timestamp across all records fetched (used for time window)
- **`continuation_token`**: Current pagination token within that time window (if any)

#### Timestamp Coalescing Per Resource
Extract timestamps using fallback logic (first non-blank value wins):
- **Executions**: `("timestamp", "info.timestamp", "takerOrder.timestamp", "trade.timestamp")`
- **Account log**: `("timestamp", "date")`
- **Position history**: `"timestamp"`

Reference: See `calculator-api/app/services/data_sync.py:192-199` for working implementation.

#### Fetch Logic (Continuation Token + Timestamp Interaction)
The continuation token and timestamp serve different purposes:
- **Timestamp (since/before)**: Defines the *time window* to fetch (e.g., "get all data from 2024-01-01 to now")
- **Continuation token**: Handles *pagination within that time window* (e.g., "this window has 10,000 records, here's page 1")

**Flow**:
1. **First run**: `since = None`, `continuation_token = None` (fetch all available history)
2. **Subsequent runs**: `since = last_timestamp` from state, `continuation_token = None` (start new time window)
3. **Within time window**: Keep using same `since` + new continuation tokens from API responses
4. **Token exhausted**: When API returns `continuation_token = None`, the time window is complete
5. **Update state**: Set `last_timestamp = max(timestamps_from_batch)`, clear `continuation_token`

**Fallback Paging** (Account Log Only):
If API returns NO continuation token BUT page is "full" (len(logs) >= count):
- Page backwards using `before = earliest_timestamp_in_page`
- This prevents gaps when Kraken's token mechanism fails
- **Safety limit**: Cap at 50 fallback pages to prevent infinite loops
- Reference: See `calculator-api/app/services/data_sync.py:317-331`

#### Data Preservation
- Always include `raw_data` (original JSON payload) for forward compatibility
- Use deterministic JSON encoding (`json.dumps(sort_keys=True)`) to avoid duplicate rows

#### Write Disposition
- **Incremental resources** (`executions`, `account_log`, `position_history`):
  - Use `write_disposition="append"` with natural dedupe keys
  - Natural keys: `uid` (executions), `booking_uid` (account_log), `executionUid` or `uid` (position_history)
  - DLT's merge strategy or destination-level `ON CONFLICT DO NOTHING` prevents duplicates
- **Snapshot resources** (`tickers`, `open_positions`):
  - Use `write_disposition="replace"` (point-in-time snapshot)

---

### Implementation Tasks

#### Phase 1: Core Infrastructure
1. **Create `kraken_futures_source/auth.py`** with custom DLT auth implementing Kraken HMAC headers
   - Reference: `calculator-api/app/services/kraken_client.py:16-73` (working nonce + HMAC implementation)
   - Key requirements:
     - Nonce: `timestamp_ms + counter.zfill(5)` with overflow handling at 99,999
     - Signature: `base64(HMAC-SHA512(base64_decode(secret), sha256(postData + nonce + endpointPath)))`
     - Headers: `APIKey`, `Authent`, `Nonce`

2. **Implement `executions` resource** (incremental + continuation token)
   - Endpoint: `GET /api/history/v2/executions`
   - State: Track `last_timestamp` and `continuation_token`
   - Timestamp coalescing: `("timestamp", "info.timestamp", "takerOrder.timestamp", "trade.timestamp")`
   - Natural key: `uid`
   - Handle response variants: `elements` or `executionEvents`

3. **Implement `account_log` resource** (incremental + token with `before` fallback)
   - Endpoint: `GET /api/history/v2/account-log`
   - State: Track `last_timestamp` and `continuation_token`
   - Timestamp coalescing: `("timestamp", "date")`
   - Natural key: `booking_uid`
   - **Fallback paging**: If no token but page is full, page backwards using `before` (max 50 iterations)
   - Reference: `calculator-api/app/services/data_sync.py:257-337`

4. **Implement `position_history` resource** (incremental + token)
   - Endpoint: `GET /api/history/v3/positions`
   - State: Track `last_timestamp` and `continuation_token`
   - Timestamp: `"timestamp"`
   - Natural key: `executionUid` or `uid`
   - Parse nested structure: `event.PositionUpdate`

5. **Implement `tickers` and `open_positions` snapshot resources**
   - Endpoints: `GET /derivatives/api/v3/tickers`, `GET /derivatives/api/v3/openpositions`
   - Write disposition: `replace`
   - No state tracking needed

6. **Wire `@dlt.source` in `__init__.py`**
   - Common base URL and auth
   - Resource selection via parameters

7. **Add `pipelines/run_pipeline.py`**
   - CLI arguments: `--resources`, `--since`, `--dev-mode`
   - Run selected resources to DuckDB
   - Log run results (row counts, duration, state)

#### Phase 2: Configuration & Secrets
8. **Provide secrets management guidance**
   - `.envrc` template with `KRAKEN_API_PUBLIC_KEY` and `KRAKEN_API_SECRET_KEY`
   - `.dlt/secrets.toml` as fallback
   - **Never commit secrets to git**

#### Phase 3: Testing
9. **Add tests with recorded fixtures**
   - **Continuation token pagination**: Test multi-page responses with tokens
   - **State resume after failure**: Verify state unchanged if pipeline fails
   - **Fallback paging**: Test account_log when token is missing but page is full
   - **Timestamp coalescing**: Verify each resource's fallback order works
   - **Deduplication**: Re-run same time window shouldn't create duplicates
   - **Nonce uniqueness**: Verify nonce generation doesn't collide
   - **Rate limit retry**: Test 429 response triggers exponential backoff
   - Store fixtures in `tests/fixtures/` as JSON files
   - Mock `httpx` client with fixture data to avoid live API calls

#### Phase 4: Observability
10. **Add monitoring and logging**
    - Log after each pipeline run:
      - Rows loaded per resource
      - Load duration
      - Current cursor state (`last_timestamp`, `continuation_token`)
      - API errors/retries
    - Optional: Configure Sentry DSN for error tracking
    - Load trace data to destination for historical observability

11. **Document state management and error recovery**
    - README section on how state persists
    - How to check current state
    - How to reset state (full or partial)
    - What happens on pipeline failure

---

### Local Usage (proposed)
```bash
uv sync
export KRAKEN_API_PUBLIC_KEY=...  # or via direnv
export KRAKEN_API_SECRET_KEY=...
uv run python -m pipelines.run_pipeline --resources executions account_log position_history --since "2024-01-01T00:00:00Z"
## schedule every 15 minutes (example cron)
# */15 * * * * cd /path/to/kraken-dlt-source && uv run python -m pipelines.run_pipeline --resources executions account_log position_history >> .dlt/cron.log 2>&1
```

DuckDB output
- Database file under `.dlt/` (default); dataset name `kraken_futures`.

---

### Risks & Mitigations

| Risk | Mitigation | Reference |
|------|------------|-----------|
| **Rate limits (429)** | Exponential backoff with jitter: 2s, 4s, 8s delays. Cap at 3 retries. If all retries fail, raise exception and let pipeline fail (state will be preserved). | `calculator-api/app/services/kraken_client.py:136-149` |
| **Inconsistent token behavior** | Token-first approach. Only use `before`-based fallback paging when: (1) no token returned AND (2) page is full (len >= count). Cap fallback at 50 iterations to prevent infinite loops. | `calculator-api/app/services/data_sync.py:317-331` |
| **Schema drift** | Always preserve `raw_data` column with full JSON payload. Normalize only cursor fields (timestamp) and natural keys (uid, booking_uid). DLT handles schema evolution automatically. | `calculator-api/app/database/models.py` |
| **Secrets handling** | Use environment variables (`KRAKEN_API_PUBLIC_KEY`, `KRAKEN_API_SECRET_KEY`). Never log secrets. Add `.env` and `.dlt/secrets.toml` to `.gitignore`. | DLT best practices |
| **Nonce collisions** | Use `timestamp_ms + counter.zfill(5)` with overflow handling. Counter resets at 99,999 but timestamp advances, preventing collisions. Thread-safe counter not needed for single-process DLT pipelines. | `calculator-api/app/services/kraken_client.py:22-29` |
| **State corruption on failure** | DLT persists state atomically with data. If pipeline fails mid-run, state remains at last successful load. No manual intervention needed. | DLT docs: State Management |
| **Duplicate records** | Use natural keys (`uid`, `booking_uid`, `executionUid`) as primary keys. DLT's merge disposition or destination-level `ON CONFLICT DO NOTHING` prevents duplicates. | `calculator-api/app/services/data_sync.py:409-410` |
| **Infinite pagination loops** | Cap fallback paging at 50 iterations with explicit counter. Log warning and break if limit reached. | `calculator-api/app/services/data_sync.py:327-330` |
| **Missing/malformed timestamps** | Use coalescing logic (first non-blank value). Skip records with no valid timestamp. Log count of skipped records for monitoring. | `calculator-api/app/services/data_sync.py:363-372` |
| **API permission errors** | Catch `permission_denied` errors gracefully. Log warning and return empty list (don't fail pipeline). This allows partial syncs if API key has limited permissions. | `calculator-api/app/services/kraken_client.py:232-234` |

---

### Best Practices for Source Connectors

#### Data Integrity
- **Preserve `raw_data`**: Store full JSON payload for forward compatibility and debugging; normalize only cursor fields and natural keys
- **Deterministic JSON encoding**: Use `json.dumps(sort_keys=True)` to avoid duplicate rows from non-deterministic key ordering
- **Idempotent loads**: Use natural stable identifiers (`uid`, `booking_uid`, `executionUid`) as primary keys for deduplication
- **Timestamp normalization**: Convert all timestamps to naive-UTC for consistent comparisons (DLT requirement)

#### State Management
- **Use resource state**: Store cursors with `dlt.current.resource_state()` for per-resource isolation
  ```python
  state = dlt.current.resource_state()
  state.setdefault("last_timestamp", None)
  state.setdefault("continuation_token", None)
  ```
- **Atomic persistence**: State only updates when data load succeeds (DLT guarantees atomicity)
- **State restoration**: DLT automatically restores state from `_dlt_pipeline_state` table on clean starts (ephemeral environments)

#### Error Handling
- **Rate limiting**: Bounded retries (3 max) with exponential backoff (2s, 4s, 8s) and jitter (±10%)
- **Transient errors**: Retry with backoff; permanent errors (permission_denied) should log warning and return empty list
- **Pagination safety**: Cap fallback paging at 50 iterations to prevent infinite loops

#### Schema Evolution
- **Destination-agnostic code**: Avoid destination-specific types; let DLT handle type mapping
- **Append-only preferred**: Add columns instead of mutating types; use `replace` disposition only for true snapshots
- **Schema flexibility**: DLT handles schema evolution automatically (new columns, type widening)

#### Testing & Observability
- **Fixture-based tests**: Use recorded API responses in `tests/fixtures/` to avoid live API calls
- **Test edge cases**: Continuation token pagination, state resume, fallback paging, timestamp coalescing, deduplication, nonce uniqueness
- **Development mode**: Use `dev_mode=True` during development to reset state between iterations
- **Trace loading**: Enable trace data loading to `_dlt_pipeline_trace` for historical observability
- **Monitoring**: Log row counts, duration, cursor state, and API errors after each run

#### Security
- **Secrets management**: Load via environment variables or `.dlt/secrets.toml` (never commit to git)
- **Never log secrets**: Sanitize logs to prevent accidental API key exposure
- **Validate secrets**: Check base64 format and minimum length before use

---

### Error Recovery and State Management

#### How DLT State Works
DLT persists pipeline state as a Python dictionary that is atomically committed with loaded data:
- **Local storage**: State stored in `.dlt/<pipeline_name>/` directory (scoped to pipeline name)
- **Remote sync**: State automatically synced to `_dlt_pipeline_state` table at destination (enabled by default)
- **Atomic commits**: State only updates if the entire load succeeds (all-or-nothing guarantee)

#### Automatic Recovery on Failure
When a pipeline fails partway through execution:

1. **State preserved**: The `last_timestamp` and `continuation_token` remain at the last successful load
2. **Data preserved**: No partial writes to destination (DLT uses staging tables)
3. **Resume on re-run**: Next execution starts from saved cursor position
4. **Deduplication**: Natural keys prevent duplicate records if re-run overlaps with previous batch

**Example**: If pipeline fails after loading 5,000 records:
```python
# Before failure:
state = {"last_timestamp": "2024-01-15T10:30:00", "continuation_token": "abc123"}

# After failure:
state = {"last_timestamp": "2024-01-15T10:30:00", "continuation_token": "abc123"}  # Unchanged

# On re-run:
# Pipeline resumes from 2024-01-15T10:30:00 with token "abc123"
# Natural keys (uid, booking_uid) prevent duplicates from overlap
```

#### Checking Current State
Inspect state before/after pipeline runs:
```python
import dlt

pipeline = dlt.pipeline(
    pipeline_name="kraken_futures",
    destination="duckdb",
    dataset_name="kraken_futures"
)

# Check pipeline state
state = pipeline.state
print(f"Executions state: {state.get('resources', {}).get('executions', {})}")
print(f"Account log state: {state.get('resources', {}).get('account_log', {})}")

# Example output:
# Executions state: {'last_timestamp': '2024-01-15T10:30:00', 'continuation_token': None}
```

#### Manual State Reset

**Full reset** (drops all data and state):
```bash
dlt pipeline kraken_futures drop --drop-all
```

**Development reset** (use during testing):
```python
pipeline = dlt.pipeline(
    pipeline_name="kraken_futures",
    destination="duckdb",
    dataset_name="kraken_futures",
    dev_mode=True  # Resets schema and state on each run
)
```

**Partial reset** (reset specific resource state):
```python
# Delete state for one resource
import dlt
pipeline = dlt.pipeline(pipeline_name="kraken_futures")
state = pipeline.state
if 'resources' in state and 'executions' in state['resources']:
    del state['resources']['executions']
pipeline.sync_state()
```

#### State in Ephemeral Environments (Airflow, Lambda)
DLT automatically handles ephemeral filesystems:
1. On first run, local `.dlt/` directory is empty
2. DLT loads state from `_dlt_pipeline_state` table at destination (if exists)
3. Pipeline executes and updates state
4. DLT saves state back to `_dlt_pipeline_state` table
5. On next run (new container), state is restored from destination

**Configuration**:
```toml
# .dlt/config.toml
[pipeline]
restore_from_destination = true  # Default; set to false to disable
```

---

### Testing Strategy

#### Test Categories

##### 1. Unit Tests (auth & utilities)
- **Nonce generation**: Verify uniqueness and overflow handling
- **HMAC signature**: Test against known-good signatures from Kraken docs
- **Timestamp coalescing**: Test fallback logic with missing fields
- **JSON encoding**: Verify deterministic serialization

##### 2. Integration Tests (resource pagination)
Use recorded fixtures from `tests/fixtures/` to mock API responses:

**Test: Continuation token pagination**
```python
# Fixture: executions_page1.json (500 records + token)
# Fixture: executions_page2.json (500 records + token)
# Fixture: executions_page3.json (100 records, no token)
# Expected: 1,100 total records loaded, state updated to last timestamp
```

**Test: Fallback paging (account_log)**
```python
# Fixture: account_log_no_token_full_page.json (500 records, no token)
# Fixture: account_log_before_page.json (400 records, no token)
# Expected: Fallback paging triggers, loads both pages, stops when page not full
```

**Test: State resume after failure**
```python
# 1. Load page 1, save state
# 2. Simulate failure (raise exception)
# 3. Verify state unchanged
# 4. Re-run pipeline
# 5. Verify resumes from page 2 (uses continuation_token)
```

**Test: Deduplication**
```python
# 1. Load time window [T1, T2]
# 2. Re-run same time window
# 3. Verify no duplicate records (query by uid/booking_uid)
```

##### 3. End-to-End Tests (with test API key)
If you have a test/sandbox Kraken account:
- Test live pagination with small time windows
- Verify state persists correctly across runs
- Test rate limit handling (intentionally exceed limits)

##### 4. Property-Based Tests (optional)
Use `hypothesis` library:
- Generate random timestamp sequences, verify coalescing always returns valid datetime
- Generate random nonce counter values, verify no collisions

#### Test Fixtures Structure
```
tests/
├── fixtures/
│   ├── executions/
│   │   ├── page1_with_token.json
│   │   ├── page2_with_token.json
│   │   └── page3_no_token.json
│   ├── account_log/
│   │   ├── with_continuation_token.json
│   │   ├── no_token_full_page.json
│   │   └── no_token_partial_page.json
│   └── position_history/
│       └── multi_page.json
├── test_auth.py
├── test_resources.py
├── test_state_management.py
└── test_pagination.py
```

#### Mocking Strategy
Mock `httpx.AsyncClient` to return fixtures:
```python
import pytest
from unittest.mock import AsyncMock, patch
import json

@pytest.fixture
def mock_httpx_client():
    with patch('httpx.AsyncClient') as mock:
        # Load fixture
        with open('tests/fixtures/executions/page1_with_token.json') as f:
            fixture_data = json.load(f)

        # Mock response
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.json.return_value = fixture_data

        mock.return_value.get.return_value = mock_response
        yield mock
```

#### Running Tests
```bash
# Run all tests
uv run pytest tests/

# Run specific test file
uv run pytest tests/test_pagination.py

# Run with coverage
uv run pytest --cov=kraken_futures_source tests/

# Run in dev mode (reset state between tests)
uv run pytest --dev-mode tests/
```

#### Test Quality Gates
Before merging to main:
- ✅ All tests pass
- ✅ Code coverage > 80%
- ✅ No skipped tests (unless explicitly documented)
- ✅ Fixtures cover all response variants (elements vs executionEvents, logs vs accountLog, etc.)

---

### Monitoring and Observability

#### Metrics to Track

##### Per-Run Metrics
Log after each `pipeline.run()` call:

```python
import dlt
from datetime import datetime

pipeline = dlt.pipeline(...)
info = pipeline.run(kraken_futures_source())

# Extract metrics
for package in info.load_packages:
    for job in package.jobs['completed_jobs']:
        table_name = job.job_file_info.table_name
        rows = job.metrics.get('rows', 0) if job.metrics else 0
        print(f"Loaded {rows} rows to {table_name}")

    duration = package.completed_at - package.started_at
    print(f"Load duration: {duration.total_seconds()}s")

# Check cursor state
state = pipeline.state
for resource_name in ['executions', 'account_log', 'position_history']:
    res_state = state.get('resources', {}).get(resource_name, {})
    print(f"{resource_name}: {res_state}")
```

**Key metrics**:
- **Rows loaded per resource**: Track volume over time, alert on zero rows
- **Load duration**: Identify performance regressions
- **Cursor lag**: `current_time - last_timestamp` (should be < 30 min for 15-min schedule)
- **API errors**: Count of retries, rate limits, permission denials
- **State changes**: Log when `last_timestamp` and `continuation_token` update

##### Historical Metrics (via trace tables)
DLT automatically populates trace tables:

```sql
-- Row count trends
SELECT
    table_name,
    DATE(started_at) as load_date,
    SUM(rows) as total_rows,
    COUNT(*) as load_count
FROM _dlt_loads
WHERE pipeline_name = 'kraken_futures'
GROUP BY table_name, DATE(started_at)
ORDER BY load_date DESC;

-- Load performance
SELECT
    table_name,
    AVG(TIMESTAMPDIFF(SECOND, started_at, completed_at)) as avg_duration_sec,
    MAX(TIMESTAMPDIFF(SECOND, started_at, completed_at)) as max_duration_sec
FROM _dlt_loads
WHERE pipeline_name = 'kraken_futures'
GROUP BY table_name;

-- Failed loads
SELECT *
FROM _dlt_loads
WHERE pipeline_name = 'kraken_futures'
  AND status = 'failed'
ORDER BY started_at DESC
LIMIT 10;
```

#### Alerting Thresholds

| Alert | Condition | Severity | Action |
|-------|-----------|----------|--------|
| **Zero rows loaded** | All resources return 0 rows for 2+ consecutive runs | High | Check API connectivity, permissions, state correctness |
| **Pipeline failure** | `pipeline.run()` raises exception | Critical | Check logs for error details, verify API is accessible |
| **Rate limit exhaustion** | All retries fail with 429 | Medium | Reduce sync frequency or increase backoff delays |
| **Stale data** | `current_time - last_timestamp > 1 hour` (for 15-min schedule) | Medium | Check if pipeline is running on schedule |
| **Excessive load duration** | Load takes > 5 minutes | Low | Investigate slow queries, API latency, or data volume spike |
| **State desync** | `continuation_token` persists for > 6 hours | Medium | Possible infinite pagination loop, check fallback paging logic |

#### Sentry Integration (Optional)
Enable error tracking and performance monitoring:

```bash
# Set environment variable
export SENTRY_DSN="https://your-sentry-dsn@sentry.io/project"
```

DLT automatically sends:
- Pipeline exceptions with full traceback
- Resource-level errors
- Performance metrics (load duration, row counts)

**Configuration**:
```toml
# .dlt/config.toml
[runtime]
sentry_dsn = "env:SENTRY_DSN"
log_level = "INFO"
```

#### Row Count Reconciliation
Periodically verify data completeness:

```python
# Compare loaded records vs API metadata (if available)
from datetime import datetime, timedelta
import dlt

pipeline = dlt.pipeline(pipeline_name="kraken_futures")

# Query loaded records
with pipeline.sql_client() as client:
    result = client.execute_sql("""
        SELECT COUNT(*) as count, MIN(timestamp) as earliest, MAX(timestamp) as latest
        FROM executions
    """)
    loaded_count, earliest, latest = result[0]

# Compare with expected (if you have a way to query Kraken for total count in time range)
# Alert if discrepancy > 1%
```

#### Dashboard Recommendations
Build a monitoring dashboard (Grafana, Metabase, etc.) with:
1. **Timeseries chart**: Rows loaded per resource per day
2. **Status panel**: Last successful run timestamp per resource
3. **Error log**: Recent failed loads with error messages
4. **Performance chart**: Load duration trend over time
5. **State panel**: Current cursor position (last_timestamp) per resource

**Data source**: Query `_dlt_loads`, `_dlt_pipeline_state`, and `_dlt_pipeline_trace` tables

---

### OpenAPI/Swagger
No official OpenAPI/Swagger spec for Kraken Futures was found. Use the published documentation for REST and WebSocket APIs and implement code-first resources.
- Reference: [Kraken Derivatives (Futures) API Documentation](https://support.kraken.com/en-au/articles/8832693094036-derivatives-api-documentation)

---

### Reference Implementations

This plan is based on proven patterns from the `kraken-futures-dashboard` repository. Key reference files:

#### Authentication & API Client
- **`calculator-api/app/services/kraken_client.py`**
  - Lines 16-73: Nonce generation and HMAC signature logic (working implementation)
  - Lines 89-187: Rate limiting with exponential backoff (429 handling)
  - Lines 210-249: Executions endpoint with continuation token
  - Lines 252-337: Account log with fallback paging
  - Lines 339-397: Position history pagination

#### Data Persistence & Cursor Management
- **`calculator-api/app/services/data_sync.py`**
  - Lines 62-96: Cursor tracking (get/update sync cursor)
  - Lines 103-223: Bulk upsert with field mapping and timestamp coalescing
  - Lines 317-331: Fallback paging logic with 50-iteration safety limit
  - Lines 444-460: Latest timestamp extraction
  - Lines 492-542: Timestamp coercion (handles ms timestamps, ISO strings)
  - Lines 560-582: Nested field access with fallback (used for coalescing)

#### Database Schema
- **`calculator-api/app/database/models.py`**
  - Shows table schemas with `raw_data` JSON column
  - Natural keys: `kraken_uid`, `kraken_booking_uid`
  - Timestamp columns as `DateTime` (naive-UTC)

#### Validation & Testing
- **`calculator-api/tests/test_incremental_sync.py`**
  - Shows testing patterns for incremental sync
  - State management validation
  - Cursor behavior verification

**Recommendation**: Clone the `kraken-futures-dashboard` repo locally and reference these files while implementing the DLT source. The patterns are proven and handle all the edge cases.

---

### Implementation Readiness Checklist

Before starting implementation, ensure:

- ✅ **calculator-api repository cloned** for reference implementations
- ✅ **DLT installed**: `pip install dlt`
- ✅ **Kraken API credentials** available (public key + secret key)
- ✅ **Test environment setup**: Python 3.9+, uv installed
- ✅ **DuckDB installed** (comes with dlt[duckdb])
- ✅ **Understanding of DLT state management** (read DLT docs: State, Incremental Loading)
- ✅ **Fixture recording plan**: Decide if using real API calls to generate fixtures or hand-crafting them

**First steps**:
1. Create project structure: `uv init kraken-dlt-source`
2. Add dependencies: `dlt[duckdb]`, `httpx`, `pytest`
3. Implement auth.py using calculator-api reference
4. Test auth with a simple tickers call (no state needed)
5. Implement executions resource with state tracking
6. Test with dev_mode=True to iterate quickly

---

### Questions & Support

**Resolved questions**:
- ✅ Continuation token + timestamp interaction (clarified in Incremental Strategy)
- ✅ Fallback paging safety (50-iteration cap from calculator-api)
- ✅ Nonce collision handling (overflow at 99,999 with timestamp advancement)
- ✅ State corruption on failure (DLT handles atomically)
- ✅ Deduplication strategy (natural keys with ON CONFLICT DO NOTHING)

**Open questions for implementer**:
- Will you generate fixtures from live API calls or hand-craft them?
- Do you want to implement all 3 incremental resources first, or one at a time?
- Should monitoring be built into `run_pipeline.py` or separate script?

**Getting help**:
- DLT Slack: https://dlthub.com/community
- DLT Docs: https://dlthub.com/docs
- Kraken API Docs: https://support.kraken.com/en-au/articles/8832693094036-derivatives-api-documentation
- Reference this plan and calculator-api implementation


