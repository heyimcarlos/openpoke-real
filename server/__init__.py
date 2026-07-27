"""OpenPoke Python server package."""

from __future__ import annotations

from typing import Any


__all__ = ["app"]


def __getattr__(name: str) -> Any:
    if name != "app":
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    from .app import app as application

    globals()["app"] = application
    return application
