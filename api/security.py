"""API-key authentication for data, metrics, and model routes."""

import secrets

from fastapi import HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader

API_KEY_HEADER = "X-API-Key"
MINIMUM_API_KEY_LENGTH = 32
api_key_header = APIKeyHeader(name=API_KEY_HEADER, auto_error=False)


def validate_auth_config(enabled: bool, api_key: str) -> None:
    """Reject enabled authentication with a missing or trivially weak key."""
    if enabled and len(api_key) < MINIMUM_API_KEY_LENGTH:
        raise RuntimeError(
            f"API authentication requires a key of at least {MINIMUM_API_KEY_LENGTH} characters"
        )


def require_api_key(
    request: Request,
    supplied_key: str | None = Security(api_key_header),
) -> None:
    """Authorize a protected request using a constant-time key comparison."""
    if not request.app.state.auth_enabled:
        return
    expected_key: str = request.app.state.api_key
    if supplied_key is None or not secrets.compare_digest(supplied_key, expected_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid or missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
