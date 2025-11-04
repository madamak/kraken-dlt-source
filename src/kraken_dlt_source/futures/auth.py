import base64
import hashlib
import hmac
import threading
import time
from typing import Dict, Mapping, Optional
from urllib.parse import urlencode

NONCE_SUFFIX_PAD = 5
NONCE_MAX = 10**NONCE_SUFFIX_PAD


class KrakenFuturesAuth:
    """Generate Kraken Futures authentication headers.

    The API secret provided by Kraken is base64 encoded. We reuse the encoding
    format documented in the public REST reference: build a nonce using the
    current millisecond epoch plus a monotonically increasing counter, hash the
    request payload, and sign it with HMAC-SHA512.  This class is intentionally
    dependency-free so it can be reused in unit tests without hitting the
    network stack.
    """

    def __init__(self, api_key: str, api_secret: str) -> None:
        if not api_key:
            raise ValueError("api_key must be provided")
        if not api_secret:
            raise ValueError("api_secret must be provided")
        self.api_key = api_key
        self._secret = base64.b64decode(api_secret)
        self._counter = 0
        self._lock = threading.Lock()

    def __call__(self, path: str, params: Optional[Mapping[str, object]] = None) -> Dict[str, str]:
        """Return the headers required for a signed Kraken Futures request."""
        nonce = self._next_nonce()
        payload = self._canonical_query(params)
        sha = hashlib.sha256((payload + nonce + path).encode("utf-8")).digest()
        digest = hmac.new(self._secret, sha, hashlib.sha512).digest()
        signature = base64.b64encode(digest).decode("ascii")

        return {
            "APIKey": self.api_key,
            "Authent": signature,
            "Nonce": nonce,
        }

    def _canonical_query(self, params: Optional[Mapping[str, object]]) -> str:
        if not params:
            return ""
        items = []
        for key in sorted(params):
            value = params[key]
            if value is None:
                continue
            items.append((key, str(value)))
        return urlencode(items)

    def _next_nonce(self) -> str:
        with self._lock:
            now_ms = int(time.time() * 1000)
            self._counter = (self._counter + 1) % NONCE_MAX
            return f"{now_ms}{self._counter:0{NONCE_SUFFIX_PAD}d}"
