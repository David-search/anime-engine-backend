"""AniList -> provider id mapping (the hard non-crypto problem).

AniList knows 'what exists' (id 154587); the provider knows it by a different id
and a slightly different title. We bridge by searching the provider with AniList's
titles and scoring candidates by name similarity + episode-count proximity. The
result is cached forever (per AniList id) — mapping is solved once per show.
"""
from __future__ import annotations

import difflib

from . import anilist
from .providers.base import AnimeProvider, ProviderBlocked, SearchResult

_cache: dict[int, str | None] = {}


def _norm(s: str) -> str:
    return "".join(c for c in (s or "").lower() if c.isalnum() or c == " ").strip()


def _pick(results: list[SearchResult], titles: list[str], episodes: int | None) -> SearchResult | None:
    if not results:
        return None
    norm_titles = [_norm(t) for t in titles if t]
    best, best_score = None, -1.0
    for r in results:
        rn = _norm(r.title)
        sim = max((difflib.SequenceMatcher(None, rn, t).ratio() for t in norm_titles), default=0.0)
        score = sim
        if episodes and r.sub:
            score -= min(abs(r.sub - episodes), 24) * 0.004  # small penalty for ep-count mismatch
        if score > best_score:
            best, best_score = r, score
    return best if best_score >= 0.5 else results[0]


async def resolve(client, provider: AnimeProvider, anilist_id: int) -> str | None:
    if anilist_id in _cache:
        return _cache[anilist_id]

    media = await anilist.get_media(client, anilist_id)
    if not media:
        _cache[anilist_id] = None
        return None

    titles = [t for t in (media.get("titleRomaji"), media.get("title"), media.get("titleNative")) if t]
    results: list[SearchResult] = []
    for t in titles:
        try:
            results = await provider.search(client, t)
        except ProviderBlocked:
            raise  # surface clearance problems instead of caching a miss
        except Exception:
            results = []
        if results:
            break

    best = _pick(results, titles, media.get("episodes"))
    _cache[anilist_id] = best.id if best else None
    return _cache[anilist_id]


def clear_cache() -> None:
    _cache.clear()
