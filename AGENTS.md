# Repository Guidelines

## Project Structure & Module Organization
- Pipeline entry point: `pipelines/run_pipeline.py` — the main pipeline orchestration script.
- For instructions on how to query DuckDB from this pipeline look at `documentation/duckdb/AGENTS.md`
- Documentation lives under `documentation/plans/` (numbered plan files). Expand with endpoint notes, auth diagrams, and runbooks as they materialize.
- Source package structure: `src/kraken_dlt_source/` is the unified package for all Kraken APIs:
  - `futures/` contains Futures API implementation (`auth.py`, `resources.py`, `__init__.py`)
  - `spot/` is reserved for future Spot API implementation
  - `common/` contains shared utilities across all Kraken APIs
- Keep pipeline orchestration (e.g., `pipelines/run_pipeline.py`) separate from reusable source logic.
- Use `dev/` (git-ignored) for debug scripts, experiments, and development artifacts that shouldn't be committed.

## Build, Test, and Development Commands
- `uv sync` — install runtime dependencies defined in `pyproject.toml` and `uv.lock`.
- `uv run python -m pipelines.run_pipeline` — execute the pipeline; use this to run the Kraken Futures data sync.
- Remember: always prefix Python invocations with `uv run` so they execute inside the project environment (`uv run python -m ...`, not plain `python`).
- `uv run pytest` — run the test suite with coverage reporting.
- `uv run python -m dlt init` — scaffold helper assets when you add new destinations or want a fresh pipeline skeleton.

## Coding Style & Naming Conventions
- Use Python 3.12 features with PEP 8 defaults: 4-space indentation, snake_case for modules/functions, PascalCase for classes, and descriptive names for resources (`executions_resource`, `account_log_resource`).
- Type hints are expected; prefer `typing` and `pydantic`-style literal types to make resource schemas explicit.
- Keep side effects (network calls, auth signing) inside dedicated helpers; reserve module top levels for configuration constants.

## Testing Guidelines
- Add `pytest`-based suites under `tests/`, mirroring package structure (e.g., `tests/test_resources_executions.py`).
- Mock Kraken API HTTP calls with recorded fixtures so pagination logic (timestamps + continuation tokens) is deterministic.
- Gate new features with `uv run pytest`; target ≥80% coverage on `src/kraken_dlt_source/` by exercising success, empty-page, and error flows.

## Commit & Pull Request Guidelines
- Use Conventional Commit prefixes (`feat:`, `fix:`, `chore:`, `docs:`, `test:`) in the imperative mood.
- Squash small WIP commits before opening PRs; document the touched endpoints, affected resources, and testing commands in the PR description.
- Reference GitHub issues directly in the commit message or PR description, and include screenshots or sample payload diffs when behavior changes.

## Security & Configuration Tips
- Never commit credentials; load via environment variables or `.dlt/secrets.toml` (git-ignored):
  - Futures: `KRAKEN_FUTURES__API_KEY` and `KRAKEN_FUTURES__API_SECRET`
  - Spot (future): `KRAKEN_SPOT__API_KEY` and `KRAKEN_SPOT__API_SECRET`
- Note the **double underscore** (`__`) in variable names - this is dlt's convention for nested configuration
- When sharing run logs, redact request signatures and timestamps that could reveal nonce patterns.
