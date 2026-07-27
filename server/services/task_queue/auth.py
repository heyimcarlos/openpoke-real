"""JWT authentication boundary for task-control-plane callers."""

from __future__ import annotations

from typing import Any

import jwt

from .models import Principal


class InvalidToken(PermissionError):
    """A bearer token could not establish a trusted principal."""


class JwtPrincipalVerifier:
    """Verify a fixed JWT trust policy and return only trusted identity claims."""

    def __init__(
        self,
        *,
        signing_key: str,
        issuer: str,
        audience: str,
    ) -> None:
        if not signing_key or not issuer or not audience:
            raise ValueError("JWT signing key, issuer, and audience are required")
        self._signing_key = signing_key
        self._issuer = issuer
        self._audience = audience

    def verify(self, token: str) -> Principal:
        try:
            claims = jwt.decode(
                token,
                self._signing_key,
                algorithms=["HS256"],
                issuer=self._issuer,
                audience=self._audience,
                options={
                    "require": ["sub", "tenant_id", "iss", "aud", "exp"],
                },
            )
            actor_id = _required_string_claim(claims, "sub")
            tenant_id = _required_string_claim(claims, "tenant_id")
            scopes = _scope_claim(claims.get("scope", ""))
        except (jwt.PyJWTError, TypeError, ValueError):
            raise InvalidToken("invalid bearer token") from None

        return Principal(
            actor_id=actor_id,
            tenant_id=tenant_id,
            scopes=scopes,
        )


def _required_string_claim(claims: dict[str, Any], name: str) -> str:
    value = claims.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _scope_claim(value: Any) -> frozenset[str]:
    if not isinstance(value, str):
        raise ValueError("scope must be a space-delimited string")
    return frozenset(value.split())
