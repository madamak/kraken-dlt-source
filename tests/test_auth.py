import base64
import hashlib
import hmac

from kraken_dlt_source.futures.auth import KrakenFuturesAuth


def test_auth_generates_signature(monkeypatch):
    api_key = "example_key"
    api_secret_raw = b"example_secret"
    api_secret = base64.b64encode(api_secret_raw).decode("ascii")

    auth = KrakenFuturesAuth(api_key, api_secret)

    monkeypatch.setattr("kraken_dlt_source.futures.auth.time.time", lambda: 1_700_000_000.0)

    params = {"symbol": "PI_XBTUSD"}
    headers = auth("/api/history/v2/executions", params)

    assert headers["APIKey"] == api_key
    assert headers["Nonce"].endswith("00001")

    payload = "symbol=PI_XBTUSD"
    nonce = headers["Nonce"]
    path = "/api/history/v2/executions"
    sha = hashlib.sha256((payload + nonce + path).encode("utf-8")).digest()
    expected_signature = base64.b64encode(hmac.new(api_secret_raw, sha, hashlib.sha512).digest()).decode("ascii")
    assert headers["Authent"] == expected_signature


def test_nonce_is_monotonic(monkeypatch):
    api_secret = base64.b64encode(b"secret").decode("ascii")
    auth = KrakenFuturesAuth("key", api_secret)
    monkeypatch.setattr("kraken_dlt_source.futures.auth.time.time", lambda: 1_700_000_001.0)

    first = auth("/path", None)["Nonce"]
    second = auth("/path", None)["Nonce"]

    assert int(second) > int(first)
