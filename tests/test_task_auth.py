from __future__ import annotations

from datetime import datetime, timedelta, timezone

import jwt
import pytest

from server.services.task_queue import (
    InvalidToken,
    JwtPrincipalVerifier,
    Principal,
)


SIGNING_KEY = "test-only-signing-key-with-at-least-32-bytes"
ISSUER = "https://auth.openpoke.test"
AUDIENCE = "openpoke-api"


def _token(**overrides: object) -> str:
    now = datetime.now(timezone.utc)
    claims: dict[str, object] = {
        "sub": "user-7",
        "tenant_id": "tenant-a",
        "scope": "tasks:create tasks:read",
        "iss": ISSUER,
        "aud": AUDIENCE,
        "exp": now + timedelta(minutes=5),
    }
    claims.update(overrides)
    return jwt.encode(claims, SIGNING_KEY, algorithm="HS256")


def test_jwt_verification_builds_principal_from_trusted_claims() -> None:
    verifier = JwtPrincipalVerifier(
        signing_key=SIGNING_KEY,
        issuer=ISSUER,
        audience=AUDIENCE,
    )

    principal = verifier.verify(_token())

    assert principal == Principal(
        actor_id="user-7",
        tenant_id="tenant-a",
        scopes=frozenset({"tasks:create", "tasks:read"}),
    )


@pytest.mark.parametrize(
    "token",
    [
        jwt.encode(
            {
                "sub": "user-7",
                "tenant_id": "tenant-a",
                "scope": "tasks:create tasks:read",
                "iss": ISSUER,
                "aud": AUDIENCE,
                "exp": datetime.now(timezone.utc) + timedelta(minutes=5),
            },
            "different-signing-key-with-at-least-32-bytes",
            algorithm="HS256",
        ),
        _token(iss="https://wrong-issuer.test"),
        _token(aud="wrong-audience"),
        _token(exp=datetime.now(timezone.utc) - timedelta(seconds=1)),
    ],
    ids=["signature", "issuer", "audience", "expiry"],
)
def test_jwt_verification_rejects_invalid_trust_claims(token: str) -> None:
    verifier = JwtPrincipalVerifier(
        signing_key=SIGNING_KEY,
        issuer=ISSUER,
        audience=AUDIENCE,
    )

    with pytest.raises(InvalidToken):
        verifier.verify(token)
