"""Catalog endpoints — served from OUR store (Mongo + ES).

Detail/search/browse/popular are pure DB reads (no AniList in the request path).
The ONE exception is `trending`, which mirrors AniList's live TRENDING_DESC ranking
(time-sensitive — not something we snapshot), cached in-process so it costs ~1
AniList call per TTL window, not per request.
"""
import time

from fastapi import APIRouter, HTTPException, Request

from .. import anilist, es
from ..db import get_db

router = APIRouter()

# Live AniList trending, cached. (ES `popularity` is all-time, so serving it as
# "trending" wrongly pins evergreen giants like One Piece at #1.)
_TRENDING_TTL = 1800  # 30 min
_trending_cache: dict = {"at": 0.0, "data": None}


def _out(doc: dict) -> dict:
    """Mongo doc -> API shape (expose AniList id as `id`)."""
    doc = dict(doc)
    if "_id" in doc:
        doc["id"] = doc.pop("_id")
    return doc


@router.get("/catalog/trending")
async def trending(request: Request, page: int = 1):
    if page == 1 and _trending_cache["data"] and time.time() - _trending_cache["at"] < _TRENDING_TTL:
        return _trending_cache["data"]
    try:
        items = await anilist.trending(request.app.state.http, page=page, per=24)
        out = {"results": items, "total": len(items), "hasNext": True}
    except Exception:
        # AniList unreachable -> fall back to ES popularity so the page still loads.
        return await es.filter_search(sort="POPULARITY_DESC", page=page, size=24)
    if page == 1:
        _trending_cache.update(at=time.time(), data=out)
    return out


@router.get("/catalog/popular")
async def popular(page: int = 1):
    return await es.filter_search(sort="POPULARITY_DESC", page=page, size=24)


@router.get("/catalog/airing")
async def airing(page: int = 1):
    return await es.filter_search(status="RELEASING", sort="SCORE_DESC", page=page, size=24)


@router.get("/catalog/genres")
async def genres():
    f = await es.facets()
    return {
        "genres": f.get("genres") or anilist.GENRES,
        "tags": f.get("tags", []),
        "years": f.get("years", []),
        "formats": f.get("formats", []),
    }


@router.get("/catalog/browse")
async def browse(q: str = "", genre: str = "", tag: str = "", fmt: str = "",
                 status: str = "", season: str = "", source: str = "", year: int = 0,
                 sort: str = "POPULARITY_DESC", page: int = 1):
    return await es.filter_search(
        q=q,
        genres=[g for g in genre.split(",") if g],
        tags=[t for t in tag.split(",") if t],
        fmt=fmt or None, status=status or None, season=season or None,
        source=source or None, year=year or None, sort=sort, page=page, size=30,
    )


@router.get("/catalog/search")
async def search(q: str = "", page: int = 1):
    if not q.strip():
        return {"results": [], "total": 0, "hasNext": False}
    return await es.filter_search(q=q, sort="POPULARITY_DESC", page=page, size=30)


@router.get("/catalog/sitemap")
async def sitemap():
    """Lightweight id+title list (all anime) for the frontend sitemap.xml."""
    db = get_db()
    if db is None:
        return {"items": []}
    cur = db.anime.find({}, {"_id": 1, "title": 1}).sort("popularity", -1)
    return {"items": [{"id": d["_id"], "title": d.get("title")} async for d in cur]}


@router.get("/catalog/anime/{anime_id}")
async def detail(anime_id: int):
    # Pure DB read — zero AniList in the request path. Enrichment (heavy fields)
    # is guaranteed by background jobs: the initial enrich pass + a periodic
    # `scripts.ingest enrich` catch-up cron for new/un-enriched titles.
    db = get_db()
    doc = await db.anime.find_one({"_id": anime_id}) if db is not None else None
    if not doc:
        raise HTTPException(404, "anime not found")
    return _out(doc)
