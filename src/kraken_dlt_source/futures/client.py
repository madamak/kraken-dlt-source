"""HTTP client for Kraken Futures API with retry logic and rate limiting."""

import logging
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional
from urllib.parse import urlencode

from dlt.sources.helpers import requests
from requests import exceptions as requests_exceptions

from .auth import KrakenFuturesAuth

BASE_URL = "https://futures.kraken.com"
LOGGER = logging.getLogger(__name__)


class _RateLimitedError(Exception):
    """Exception raised when API returns HTTP 429 rate limit error."""

    def __init__(self, message: str, *, response: Optional[requests.Response] = None) -> None:
        super().__init__(message)
        self.response = response


@dataclass(slots=True)
class KrakenFuturesClient:
    """HTTP client for Kraken Futures API with exponential backoff and jitter.

    Parameters
    ----------
    auth:
        Optional KrakenFuturesAuth instance for private endpoint authentication.
    session:
        Optional requests.Session. If not provided, a new session is created.
    max_retries:
        Maximum number of retry attempts for failed requests (default: 3).
    initial_delay:
        Initial delay in seconds before first retry (default: 2.0).
    backoff_factor:
        Exponential backoff multiplier (default: 2.0).
    jitter_ratio:
        Random jitter as fraction of delay (default: 0.1).
    timeout:
        Request timeout in seconds (default: 30.0).
    min_request_interval:
        Minimum delay between successful requests to avoid rate limits (default: 0.5).
    """

    auth: Optional[KrakenFuturesAuth] = None
    session: Optional[requests.Session] = None
    max_retries: int = 3
    initial_delay: float = 2.0
    backoff_factor: float = 2.0
    jitter_ratio: float = 0.1
    timeout: float = 30.0
    min_request_interval: float = 0.5

    def __post_init__(self) -> None:
        if self.session is None:
            self.session = requests.Session()

    def get(
        self,
        path: str,
        params: Optional[Mapping[str, Any]] = None,
        *,
        private: bool = False,
    ) -> Mapping[str, Any]:
        """Execute GET request with retry logic and rate limit handling.

        Parameters
        ----------
        path:
            API endpoint path (e.g., "/api/history/v2/executions").
        params:
            Optional query parameters.
        private:
            If True, sign request using KrakenFuturesAuth (requires auth to be set).

        Returns
        -------
        Mapping[str, Any]
            JSON response payload.

        Raises
        ------
        ValueError:
            If private=True but auth is not configured.
        RuntimeError:
            If request fails after all retries or API returns error payload.
        """
        # For derivatives endpoints, strip /derivatives prefix for auth signing
        auth_path = path.replace("/derivatives", "") if path.startswith("/derivatives") else path

        # Build query string manually to ensure consistent encoding between auth and request
        query_string = ""
        if params:
            # Filter out None values and convert to strings
            filtered = {k: str(v) for k, v in params.items() if v is not None}
            if filtered:
                query_string = urlencode(sorted(filtered.items()))

        # Build full URL with query string
        url = f"{BASE_URL}{path}"
        if query_string:
            url = f"{url}?{query_string}"

        headers: Dict[str, str] = {}
        if private:
            if not self.auth:
                raise ValueError("Private endpoints require KrakenFuturesAuth")
            # Pass params dict to auth for signature generation
            headers.update(self.auth(auth_path, params))

        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                # Don't pass params to requests.get since we already added them to URL
                response = self.session.get(url, headers=headers, timeout=self.timeout)

                if response.status_code == 429:
                    raise _RateLimitedError("HTTP 429 Too Many Requests", response=response)

                response.raise_for_status()
                payload = response.json()
                try:
                    _raise_if_error(payload)
                except RuntimeError as err:
                    message = str(err).lower()
                    if "permission" in message:
                        LOGGER.warning(
                            "Permission denied for %s; returning empty result.",
                            path,
                        )
                        return {}
                    raise
                if attempt:
                    LOGGER.info(
                        "Recovered after retry %s for %s", attempt, path
                    )
                # Add small delay after successful request to avoid rate limits
                if self.min_request_interval > 0:
                    time.sleep(self.min_request_interval)
                return payload
            except (_RateLimitedError, requests_exceptions.RequestException, ValueError) as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    break
                delay = self._compute_delay(attempt)
                LOGGER.warning(
                    "Retrying %s due to %s (attempt %s/%s, delay %.2fs)",
                    path,
                    exc,
                    attempt + 1,
                    self.max_retries,
                    delay,
                )
                time.sleep(delay)
                continue

        raise RuntimeError(f"Failed to fetch {path}: {last_error}")

    def _compute_delay(self, attempt: int) -> float:
        """Calculate exponential backoff delay with jitter."""
        base = self.initial_delay * (self.backoff_factor ** attempt)
        jitter = base * self.jitter_ratio * random.uniform(-1, 1)
        return max(0.0, base + jitter)


def _raise_if_error(payload: Mapping[str, Any]) -> None:
    """Check Kraken Futures response payload for error indicators.

    Raises
    ------
    ValueError:
        If payload is None or empty.
    RuntimeError:
        If payload contains Kraken error response patterns.
    """
    if payload is None:
        raise ValueError("Empty response payload")
    # Handle common patterns returned by Kraken Futures
    if isinstance(payload.get("result"), str) and payload["result"].lower() == "error":
        error = payload.get("error") or payload
        raise RuntimeError(f"Kraken Futures API error: {error}")
    if payload.get("success") is False:
        raise RuntimeError(f"Kraken Futures API error: {payload}")


__all__ = ["KrakenFuturesClient", "BASE_URL"]
