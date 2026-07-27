"""Mint a short-lived development token for the server-side web proxy."""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone

import jwt

from server.config import get_settings


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Mint a local OpenPoke chat JWT"
    )
    parser.add_argument("--actor", default="local-user")
    parser.add_argument("--tenant", default="local")
    parser.add_argument("--hours", type=int, default=8)
    args = parser.parse_args()
    if args.hours < 1 or args.hours > 24:
        parser.error("--hours must be between 1 and 24")

    settings = get_settings()
    required = {
        "OPENPOKE_JWT_SIGNING_KEY": settings.jwt_signing_key,
        "OPENPOKE_JWT_ISSUER": settings.jwt_issuer,
        "OPENPOKE_JWT_AUDIENCE": settings.jwt_audience,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        parser.error("missing " + ", ".join(missing))

    now = datetime.now(timezone.utc)
    claims = {
        "sub": args.actor,
        "tenant_id": args.tenant,
        "scope": "chat:send",
        "iss": settings.jwt_issuer,
        "aud": settings.jwt_audience,
        "iat": now,
        "exp": now + timedelta(hours=args.hours),
    }
    if settings.jwt_signing_key is None:
        raise RuntimeError("JWT signing key is not configured")
    local_composio_id = os.getenv(
        "OPENPOKE_LOCAL_COMPOSIO_USER_ID"
    )
    if local_composio_id:
        claims["composio_user_id"] = local_composio_id
    print(
        jwt.encode(
            claims,
            settings.jwt_signing_key,
            algorithm="HS256",
        )
    )


if __name__ == "__main__":
    main()
