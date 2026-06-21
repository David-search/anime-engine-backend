"""Catalog endpoints — thin wrappers over AniList GraphQL."""
from fastapi import APIRouter, HTTPException, Request

from .. import anilist

router = APIRouter()


@router.get("/catalog/trending")
async def trending(request: Request, page: int = 1):
    return {"results": await anilist.trending(request.app.state.http, page)}


@router.get("/catalog/popular")
async def popular(request: Request, page: int = 1):
    return {"results": await anilist.popular(request.app.state.http, page)}


@router.get("/catalog/airing")
async def airing(request: Request, page: int = 1):
    return {"results": await anilist.airing(request.app.state.http, page)}


@router.get("/catalog/genres")
async def genres():
    return {"genres": anilist.GENRES}


@router.get("/catalog/browse")
async def browse(request: Request, genre: str = "", fmt: str = "", sort: str = "POPULARITY_DESC", page: int = 1):
    results = await anilist.browse(
        request.app.state.http, genre=genre or None, fmt=fmt or None, sort=[sort], page=page
    )
    return {"results": results}


@router.get("/catalog/search")
async def search(request: Request, q: str = "", page: int = 1):
    if not q.strip():
        return {"results": []}
    return {"results": await anilist.search(request.app.state.http, q, page)}


@router.get("/catalog/anime/{anime_id}")
async def detail(request: Request, anime_id: int):
    m = await anilist.get_media(request.app.state.http, anime_id)
    if not m:
        raise HTTPException(404, "anime not found")
    return m
