"""Activity boundary for search-index maintenance after a successful sync."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from temporalio import activity


@dataclass(frozen=True)
class RefreshSearchIndexInput:
    store_key: str
    lifecycle_generation: int
    sync_sequence: str


class SearchIndexRefresher(Protocol):
    async def refresh(self, command: RefreshSearchIndexInput) -> None: ...


class SearchIndexActivities:
    def __init__(self, refresher: SearchIndexRefresher) -> None:
        self._refresher = refresher

    @activity.defn(name="refresh_search_index")
    async def refresh_search_index(self, command: RefreshSearchIndexInput) -> None:
        activity.heartbeat("refreshing-lakebase-search-index")
        await self._refresher.refresh(command)
        activity.heartbeat("lakebase-search-index-ready")


__all__ = ["RefreshSearchIndexInput", "SearchIndexActivities", "SearchIndexRefresher"]
