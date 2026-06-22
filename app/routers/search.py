"""ES-powered autosuggest + search, with AniList fallback when ES has no hits."""
from fastapi import APIRouter, Request

from .. import anilist, es

router = APIRouter()


@router.get("/suggest")
async def suggest(q: str = ""):
    return {"results": await es.suggest(q)}


@router.get("/search")
async def search(request: Request, q: str = ""):
    if not q.strip():
        return {"results": [], "via": "none"}
    try:
        hits = await es.search(q)
    except Exception:
        hits = []
    if hits:
        return {"results": hits, "via": "es"}
    results = await anilist.search(request.app.state.http, q)
    return {"results": results, "via": "anilist"}
