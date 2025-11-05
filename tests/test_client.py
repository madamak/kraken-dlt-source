import base64
import logging

import pytest
from requests import exceptions as requests_exceptions

from kraken_dlt_source.futures.auth import KrakenFuturesAuth
from kraken_dlt_source.futures.client import KrakenFuturesClient


class StubResponse:
    def __init__(self, status_code, json_data=None):
        self.status_code = status_code
        self._json = json_data or {}

    def raise_for_status(self):
        if 400 <= self.status_code < 600 and self.status_code != 429:
            raise requests_exceptions.HTTPError(f"HTTP {self.status_code}", response=self)

    def json(self):
        return self._json


class StubSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
        if not self._responses:
            raise AssertionError("No responses left")
        return self._responses.pop(0)


def make_client(responses, **kwargs):
    return KrakenFuturesClient(
        auth=kwargs.get("auth"),
        session=StubSession(responses),
        max_retries=kwargs.get("max_retries", 2),
        initial_delay=kwargs.get("initial_delay", 0.1),
        backoff_factor=kwargs.get("backoff_factor", 2.0),
        jitter_ratio=0.0,
    )


def test_client_retries_on_rate_limit(monkeypatch):
    responses = [
        StubResponse(429, {"error": "rate limit"}),
        StubResponse(200, {"tickers": []}),
    ]
    client = make_client(responses)

    monkeypatch.setattr("kraken_dlt_source.futures.client.time.sleep", lambda _: None)
    monkeypatch.setattr("kraken_dlt_source.futures.client.random.uniform", lambda *_: 0)

    payload = client.get("/derivatives/api/v3/tickers")
    assert payload["tickers"] == []

    session = client.session
    assert len(session.calls) == 2
    assert session.calls[0]["headers"] == {}


def test_client_raises_after_retries(monkeypatch):
    responses = [
        StubResponse(500),
        StubResponse(500),
        StubResponse(500),
    ]
    client = make_client(responses, max_retries=2)

    monkeypatch.setattr("kraken_dlt_source.futures.client.time.sleep", lambda _: None)
    monkeypatch.setattr("kraken_dlt_source.futures.client.random.uniform", lambda *_: 0)

    with pytest.raises(RuntimeError) as exc:
        client.get("/api/history/v2/executions", private=False)

    assert "Failed to fetch" in str(exc.value)


def test_private_requests_require_auth(monkeypatch):
    responses = [
        StubResponse(200, {"openPositions": []}),
    ]
    client = make_client(responses, auth=KrakenFuturesAuth("key", base64.b64encode(b"secret").decode("ascii")))

    monkeypatch.setattr("kraken_dlt_source.futures.client.time.sleep", lambda _: None)
    payload = client.get("/derivatives/api/v3/openpositions", private=True)
    assert payload["openPositions"] == []
    assert "APIKey" in client.session.calls[0]["headers"]


def test_client_detects_error_payload(monkeypatch):
    responses = [
        StubResponse(200, {"result": "error", "error": "invalid"}),
    ]
    client = make_client(responses)

    monkeypatch.setattr("kraken_dlt_source.futures.client.time.sleep", lambda _: None)

    with pytest.raises(RuntimeError) as exc:
        client.get("/api/history/v2/executions")

    assert "invalid" in str(exc.value)


def test_client_permission_denied_returns_empty(monkeypatch, caplog):
    responses = [
        StubResponse(200, {"result": "error", "error": "permission_denied"}),
    ]
    api_secret = base64.b64encode(b"secret").decode("ascii")
    client = make_client(responses, auth=KrakenFuturesAuth("key", api_secret))

    monkeypatch.setattr("kraken_dlt_source.futures.client.time.sleep", lambda _: None)
    caplog.set_level(logging.WARNING, logger="kraken_dlt_source.futures.resources")

    payload = client.get("/api/history/v2/executions", private=True)
    assert payload == {}
    assert "permission denied" in caplog.text.lower()
