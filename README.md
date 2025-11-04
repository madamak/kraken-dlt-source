# Kraken REST API DLT Source

![Python Version](https://img.shields.io/badge/python-3.12+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Status](https://img.shields.io/badge/status-alpha-yellow.svg)
![dlt](https://img.shields.io/badge/dlt-1.18.2+-purple.svg)

This repository implements an incremental [dlt](https://dlthub.com/) connector for the Kraken Futures REST API. It focuses on Futures endpoints (executions, account log, position history) with snapshot resources for tickers and open positions.

## Quickstart

```bash
# Install dependencies
uv sync

# Configure Futures credentials
export KRAKEN_FUTURES__API_KEY="your_futures_public_key"
export KRAKEN_FUTURES__API_SECRET="your_futures_base64_secret"

# Run first sync (from last 7 days)
uv run python -m pipelines.run_pipeline \
  --resources executions \
  --since "2024-01-01T00:00:00Z"

# Subsequent runs automatically resume from last cursor
uv run python -m pipelines.run_pipeline --resources executions
```

Data is stored in `.dlt/kraken_futures.duckdb`. See [Running the pipeline](#running-the-pipeline) for all options.

## Project layout

- `src/kraken_dlt_source/` – unified source package:
  - `futures/` – Kraken Futures API implementation
    - `auth.py` signs private REST calls (nonce + HMAC-SHA512)
    - `resources.py` defines incremental/snapshot resources and shared HTTP client logic
    - `__init__.py` wires the public `kraken_futures_source()` factory
  - `spot/` – (Coming soon) Kraken Spot API implementation
  - `common/` – shared utilities across all Kraken APIs
- `pipelines/run_pipeline.py` – CLI entry point with logging, JSON output, and resource selection flags
- `tests/` – pytest suites backed by recorded fixtures under `tests/fixtures/`
- `documentation/plans/` – numbered plan files with implementation details and reference notes

## Requirements

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) for environment management (`pip install uv`).
- DuckDB is used as the default destination; no external service is required.

## Setup

```bash
uv sync           # install runtime + dev dependencies
cp .env.example .env  # populate with secrets (create file if needed)
```

Export the Kraken Futures credentials before running private resources:

```bash
export KRAKEN_FUTURES__API_KEY=...
export KRAKEN_FUTURES__API_SECRET=...
```

Note the **double underscore** (`__`) - this is dlt's convention for nested configuration.

`KRAKEN_FUTURES__API_SECRET` must be the base64-encoded secret provided by Kraken Futures. Never commit secrets; `.gitignore` already excludes `.env` and `.dlt/`.

**When Spot support is added**, you'll use separate credentials:
```bash
export KRAKEN_SPOT__API_KEY=...
export KRAKEN_SPOT__API_SECRET=...
```

## Running the pipeline

```bash
uv run python -m pipelines.run_pipeline \
  --resources executions account_log position_history \
  --since "2024-01-01T00:00:00Z" \
  --strict \
  --json
```

Flags:
- `--resources` filters the `@dlt.resource`s to load (defaults to all).
- `--since` seeds the first incremental run (state will persist afterwards).
- `--strict` raises if any job fails.
- `--json` prints the structured `LoadInfo` payload; logs always include per-resource row counts and last cursor.

For snapshots only:

```bash
uv run python -m pipelines.run_pipeline --resources tickers open_positions
```

## Checking Pipeline State

After a run, inspect cursor positions:

```bash
uv run python -c "
import dlt
pipeline = dlt.pipeline(pipeline_name='kraken_futures')
state = pipeline.state
for resource in ['executions', 'account_log', 'position_history']:
    print(f'{resource}:', state.get('resources', {}).get(resource, {}))
"
```

Reset all state and data:

```bash
dlt pipeline kraken_futures drop --drop-all
```

Reset just one resource (requires Python):

```python
import dlt

pipeline = dlt.pipeline(pipeline_name='kraken_futures')
if 'executions' in pipeline.state.get('resources', {}):
    del pipeline.state['resources']['executions']
pipeline.sync_state()
```

## Testing

Run the full suite (unit + integration-style fixtures):

```bash
uv run pytest
```

Key scenarios covered:
- Pagination and state persistence for incremental resources.
- Fallback paging (account log) and nested normalization (position history).
- Auth header signing, retry/backoff behaviour, and error handling.

## Maintenance notes

- The client already implements exponential backoff with jitter for HTTP 429/5xx responses.
- Resource logs include row counts and cursor checkpoints; use them for monitoring.
- Extend `tests/fixtures/` with new payloads when adding endpoints or edge cases (e.g., API permission failures, malformed records).

## Test Coverage

Run tests with coverage:

```bash
uv run pytest --cov=kraken_dlt_source --cov-report=term-missing
```

## Roadmap

### Current Scope
- ✅ Kraken Futures REST API
  - Incremental: executions, account log, position history
  - Snapshots: tickers, open positions

### Future Additions
- More endpoints for the futures REST API
- 🔄 Kraken Spot REST API (planned)
  - Account balances, trade history, order history
  - Ledger entries, deposits, withdrawals

Contributions welcome! Open an issue to discuss new features or improvements.

## Credentials Configuration

### Environment Variables

dlt uses a double underscore (`__`) convention for nested configuration:

```bash
# Futures API
export KRAKEN_FUTURES__API_KEY="your_futures_public_key"
export KRAKEN_FUTURES__API_SECRET="your_futures_base64_secret"

# Spot API (when available)
export KRAKEN_SPOT__API_KEY="your_spot_public_key"
export KRAKEN_SPOT__API_SECRET="your_spot_secret"
```

### secrets.toml

Alternatively, create `.dlt/secrets.toml`:

```toml
[kraken_futures]
api_key = "your_futures_public_key"
api_secret = "your_futures_base64_secret"

[kraken_spot]
api_key = "your_spot_public_key"
api_secret = "your_spot_secret"
```

This allows you to run both Futures and Spot sources with separate credentials in the same pipeline.

## License

MIT License - see [LICENSE](LICENSE) for details.
