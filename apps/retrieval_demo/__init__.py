"""Lakebase and Temporal demonstration application."""

from typing import Any

__all__ = ["create_app"]


def __getattr__(name: str) -> Any:
    """Keep the convenience export without importing the executable module eagerly."""

    if name == "create_app":
        from apps.retrieval_demo.app import create_app

        return create_app
    raise AttributeError(name)
