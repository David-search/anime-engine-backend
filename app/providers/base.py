"""The provider abstraction — the spine of the whole app.

Every stream source (allanime, animepahe, gogoanime, a HiAnime successor, …)
implements `AnimeProvider`. The rest of the app only ever sees these normalized
types, so a dead provider can be swapped without touching the UI or the proxy.

Phase 0 ships only the interface + types; Phase 1 adds the first real impl
(allanime). Until then, /api/sources serves a stub test stream.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Literal, Optional

import httpx
from pydantic import BaseModel

Category = Literal["sub", "dub", "raw"]


class ProviderError(Exception):
    """Generic provider failure (parse error, no sources, etc.)."""


class ProviderBlocked(ProviderError):
    """Upstream is behind an anti-bot wall and we lack valid clearance."""


class Subtitle(BaseModel):
    url: str
    lang: str
    default: bool = False


class Server(BaseModel):
    name: str
    category: Category = "sub"


class Episode(BaseModel):
    id: str               # provider-internal episode id
    number: float
    title: Optional[str] = None


class SearchResult(BaseModel):
    id: str               # provider-internal anime id
    title: str
    poster: Optional[str] = None
    sub: Optional[int] = None
    dub: Optional[int] = None


class AnimeInfo(BaseModel):
    id: str
    title: str
    description: Optional[str] = None
    poster: Optional[str] = None
    total_episodes: Optional[int] = None


class Skip(BaseModel):
    start: float
    end: float


class Sources(BaseModel):
    url: str                                  # master .m3u8 (still Referer-locked)
    is_m3u8: bool = True
    headers: dict = {}                        # Referer/Origin/UA to replay via proxy
    subtitles: list[Subtitle] = []
    intro: Optional[Skip] = None
    outro: Optional[Skip] = None


class AnimeProvider(ABC):
    """Implement this per stream source. `client` is a shared httpx.AsyncClient."""

    id: str
    # True when the provider is keyed by AniList id directly (no AniList->provider
    # title mapping needed). episode_id is then "{anilistId}/{ep}".
    anilist_native: bool = False

    @abstractmethod
    async def search(self, client: httpx.AsyncClient, query: str) -> list[SearchResult]: ...

    @abstractmethod
    async def info(self, client: httpx.AsyncClient, anime_id: str) -> AnimeInfo: ...

    @abstractmethod
    async def episodes(self, client: httpx.AsyncClient, anime_id: str) -> list[Episode]: ...

    @abstractmethod
    async def servers(
        self, client: httpx.AsyncClient, episode_id: str, category: Category
    ) -> list[Server]: ...

    @abstractmethod
    async def sources(
        self, client: httpx.AsyncClient, episode_id: str, server: str, category: Category
    ) -> Sources: ...
