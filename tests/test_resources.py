from __future__ import annotations

import base64
import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterator, Mapping, Optional

import dlt
import pytest

from dlt.pipeline.exceptions import PipelineStepFailed

from kraken_dlt_source.futures.resources import (
    ALL_RESOURCE_NAMES,
    executions,
    account_log,
    position_history,
    open_positions,
    KrakenFuturesClient,
    tickers,
)
from kraken_dlt_source.futures.auth import KrakenFuturesAuth


FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> Mapping[str, Any]:
    with open(FIXTURES_DIR / name, "r", encoding="utf-8") as fp:
        return json.load(fp)


class FakeClient(KrakenFuturesClient):
    def __init__(self, responses: Iterator[Mapping[str, Any]]):
        super().__init__(auth=None)
        self._responses = list(responses)
        self._calls: list[Dict[str, Any]] = []

    @property
    def calls(self) -> list[Dict[str, Any]]:
        return self._calls

    def get(
        self,
        path: str,
        params: Optional[Mapping[str, Any]] = None,
        *,
        private: bool = False,
    ) -> Mapping[str, Any]:
        self._calls.append({"path": path, "params": dict(params or {}), "private": private})
        if not self._responses:
            raise RuntimeError("No more responses registered in FakeClient")
        payload = self._responses.pop(0)
        if isinstance(payload, Exception):
            raise payload
        if isinstance(payload, Mapping):
            result = payload.get("result")
            if isinstance(result, str) and result.lower() == "error":
                message = str(payload.get("error") or payload)
                if "permission" in message.lower():
                    logging.getLogger("kraken_futures_source.resources").warning(
                        "Permission denied for %s; returning empty result.", path
                    )
                    return {}
                raise RuntimeError(message)
            if payload.get("success") is False:
                message = str(payload.get("error") or payload)
                raise RuntimeError(message)
        return payload


@pytest.fixture(autouse=True)
def reset_pipeline_state(tmp_path, monkeypatch):
    # Each test uses an isolated .dlt folder to avoid state leakage.
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path))


def make_auth() -> KrakenFuturesAuth:
    return KrakenFuturesAuth("key", base64.b64encode(b"secret").decode("ascii"))


def test_executions_paginates_and_updates_state():
    responses = iter(
        [
            load_fixture("executions_page1.json"),
            load_fixture("executions_page2.json"),
        ]
    )
    client = FakeClient(responses)

    resource = executions(auth=make_auth(), start_timestamp=None, page_size=2, client=client)
    pipeline = dlt.pipeline(
        pipeline_name="test_executions",
        destination="duckdb",
        dataset_name="test_executions",
        dev_mode=True,
    )

    pipeline.run(resource)

    executed = client.calls
    assert executed[0]["params"]["count"] == 2
    assert executed[0]["private"] is True

    with pipeline.sql_client() as sql:
        count = sql.execute_sql("SELECT COUNT(*) FROM executions")[0][0]
    assert count == 3

    state = pipeline.state["sources"]["test_executions"]["resources"]["executions"]
    assert state["last_timestamp"] == str(1_700_000_200_000)
    assert state["continuation_token"] is None


def test_tickers_is_public_and_yields_raw_data():
    responses = iter([load_fixture("tickers.json")])
    client = FakeClient(responses)

    rows = list(tickers(client=client))

    assert len(rows) == 2
    assert all("raw_data" in row for row in rows)
    assert client.calls[0]["private"] is False


def test_all_resource_names_are_unique():
    assert sorted(set(ALL_RESOURCE_NAMES)) == sorted(ALL_RESOURCE_NAMES)


def test_executions_includes_start_timestamp_on_first_run():
    responses = iter(
        [
            load_fixture("executions_page1.json"),
            load_fixture("executions_page2.json"),
        ]
    )
    client = FakeClient(responses)

    resource = executions(
        auth=make_auth(),
        start_timestamp="2023-11-14T22:13:20Z",
        page_size=2,
        client=client,
    )
    pipeline = dlt.pipeline(
        pipeline_name="test_executions_seed",
        destination="duckdb",
        dataset_name="test_executions_seed",
        dev_mode=True,
    )

    pipeline.run(resource)

    with pipeline.sql_client() as sql:
        count = sql.execute_sql("SELECT COUNT(*) FROM executions")[0][0]
    assert count == 3


def test_account_log_fallback_uses_before():
    responses = iter(
        [
            load_fixture("account_log_page1.json"),
            load_fixture("account_log_page2.json"),
        ]
    )
    client = FakeClient(responses)

    resource = account_log(auth=make_auth(), page_size=2, client=client)
    pipeline = dlt.pipeline(
        pipeline_name="test_account_log",
        destination="duckdb",
        dataset_name="test_account_log",
        dev_mode=True,
    )

    pipeline.run(resource)

    calls = client.calls
    assert calls[0]["params"].get("before") is None
    assert calls[1]["params"].get("before") == 1_699_057_000_000

    with pipeline.sql_client() as sql:
        count = sql.execute_sql("SELECT COUNT(*) FROM account_log")[0][0]
    assert count == 3

    state = pipeline.state["sources"]["test_account_log"]["resources"]["account_log"]
    assert state["last_timestamp"] == "1700000000000"


def test_account_log_keeps_new_records_on_boundary_page():
    page1 = {
        "accountLog": [
            {"booking_uid": "uid-newer", "timestamp": 1_700_000_005_000, "asset": "usd"},
            {"booking_uid": "uid-boundary-a", "timestamp": 1_700_000_000_000, "asset": "usd"},
            {"booking_uid": "uid-boundary-b", "timestamp": 1_700_000_000_000, "asset": "usd"},
        ]
    }
    page2 = {
        "accountLog": [
            {"booking_uid": "uid-boundary-a", "timestamp": 1_700_000_000_000, "asset": "usd"},
            {"booking_uid": "uid-boundary-c", "timestamp": 1_700_000_000_000, "asset": "usd"},
        ]
    }
    responses = iter([page1, page2])
    client = FakeClient(responses)

    resource = account_log(auth=make_auth(), page_size=3, client=client)
    pipeline = dlt.pipeline(
        pipeline_name="test_account_log_boundary",
        destination="duckdb",
        dataset_name="test_account_log_boundary",
        dev_mode=True,
    )

    pipeline.run(resource)

    with pipeline.sql_client() as sql:
        count = sql.execute_sql("SELECT COUNT(*) FROM account_log")[0][0]
    # We keep the two unique boundary records, the newer record, and the
    # replayed-but-new boundary record while skipping only the true duplicate.
    assert count == 4


def test_position_history_flattens_nested_events():
    responses = iter([load_fixture("position_history_page1.json")])
    client = FakeClient(responses)
    resource = position_history(auth=make_auth(), client=client)
    pipeline = dlt.pipeline(
        pipeline_name="test_position_history",
        destination="duckdb",
        dataset_name="test_position_history",
        dev_mode=True,
    )

    pipeline.run(resource)

    with pipeline.sql_client() as sql:
        rows = sql.execute_sql("SELECT uid, execution_uid FROM position_history")
    assert rows[0][0] == "abc"
    assert rows[0][1] == "xyz"


def test_open_positions_requires_auth():
    with pytest.raises(ValueError):
        list(open_positions.__wrapped__(auth=None, client=FakeClient(iter([]))))

    responses = iter([load_fixture("open_positions.json")])
    client = FakeClient(responses)
    rows = list(open_positions.__wrapped__(auth=make_auth(), client=client))
    assert rows[0]["symbol"] == "PF_XBTUSD"


def test_logging_records_counts(caplog):
    responses = iter(
        [
            load_fixture("executions_page1.json"),
            load_fixture("executions_page2.json"),
        ]
    )
    client = FakeClient(responses)
    resource = executions(auth=make_auth(), client=client)
    pipeline = dlt.pipeline(
        pipeline_name="test_logging",
        destination="duckdb",
        dataset_name="test_logging",
        dev_mode=True,
    )

    caplog.set_level(logging.INFO, logger="kraken_dlt_source.futures.resources")
    pipeline.run(resource)

    messages = [record.getMessage() for record in caplog.records]
    assert any("Resource executions loaded 3 rows" in message for message in messages)
    assert any("last_timestamp=1700000200000" in message for message in messages)


def test_resource_raises_on_error_payload():
    responses = iter([
        load_fixture("executions_error.json"),
    ])
    client = FakeClient(responses)
    resource = executions(auth=make_auth(), client=client)
    pipeline = dlt.pipeline(
        pipeline_name="test_error_payload",
        destination="duckdb",
        dataset_name="test_error_payload",
        dev_mode=True,
    )

    with pytest.raises(PipelineStepFailed):
        pipeline.run(resource)


def test_permission_denied_returns_empty(caplog):
    responses = iter([
        load_fixture("executions_permission_denied.json"),
    ])
    client = FakeClient(responses)
    resource = executions(auth=make_auth(), client=client)
    pipeline = dlt.pipeline(
        pipeline_name="test_permission_denied",
        destination="duckdb",
        dataset_name="test_permission_denied",
        dev_mode=True,
    )

    caplog.set_level(logging.WARNING, logger="kraken_futures_source.resources")
    pipeline.run(resource)

    assert "permission denied" in caplog.text.lower()
    state = pipeline.state.get("sources", {}).get("test_permission_denied", {}).get("resources", {})
    resource_state = state.get("executions")
    assert resource_state is not None
    assert resource_state.get("last_timestamp") is None
