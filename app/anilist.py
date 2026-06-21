"""AniList GraphQL client — the catalog source of truth ('what anime exists').

We only read public metadata here; this layer is fully legitimate.
"""
from __future__ import annotations

import httpx

ANILIST_URL = "https://graphql.anilist.co"

MEDIA_FIELDS = """
id
idMal
title { romaji english native }
coverImage { extraLarge large color }
bannerImage
description(asHtml: false)
episodes
format
status
seasonYear
genres
averageScore
duration
nextAiringEpisode { episode airingAt }
"""

PAGE_Q = f"""
query ($page:Int,$perPage:Int,$sort:[MediaSort]) {{
  Page(page:$page, perPage:$perPage) {{
    pageInfo {{ currentPage hasNextPage total }}
    media(type: ANIME, sort:$sort, isAdult:false) {{ {MEDIA_FIELDS} }}
  }}
}}"""

SEARCH_Q = f"""
query ($page:Int,$perPage:Int,$search:String) {{
  Page(page:$page, perPage:$perPage) {{
    pageInfo {{ currentPage hasNextPage total }}
    media(type: ANIME, search:$search, sort:SEARCH_MATCH, isAdult:false) {{ {MEDIA_FIELDS} }}
  }}
}}"""

DETAIL_Q = f"""
query ($id:Int) {{ Media(id:$id, type:ANIME) {{ {MEDIA_FIELDS} }} }}"""


async def _query(client: httpx.AsyncClient, query: str, variables: dict) -> dict:
    r = await client.post(
        ANILIST_URL,
        json={"query": query, "variables": variables},
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    r.raise_for_status()
    data = r.json()
    if data.get("errors"):
        raise RuntimeError(f"AniList error: {data['errors']}")
    return data["data"]


def _norm(m: dict | None) -> dict | None:
    if not m:
        return None
    t = m.get("title") or {}
    ci = m.get("coverImage") or {}
    nae = m.get("nextAiringEpisode")
    return {
        "id": m["id"],
        "idMal": m.get("idMal"),
        "title": t.get("english") or t.get("romaji") or t.get("native") or "Untitled",
        "titleRomaji": t.get("romaji"),
        "titleNative": t.get("native"),
        "description": m.get("description"),
        "poster": ci.get("extraLarge") or ci.get("large"),
        "banner": m.get("bannerImage"),
        "color": ci.get("color"),
        "episodes": m.get("episodes"),
        "format": m.get("format"),
        "status": m.get("status"),
        "year": m.get("seasonYear"),
        "genres": m.get("genres") or [],
        "score": m.get("averageScore"),
        "duration": m.get("duration"),
        "nextAiring": {"episode": nae["episode"], "airingAt": nae["airingAt"]} if nae else None,
    }


async def trending(client, page: int = 1, per: int = 24) -> list[dict]:
    d = await _query(client, PAGE_Q, {"page": page, "perPage": per, "sort": ["TRENDING_DESC", "POPULARITY_DESC"]})
    return [_norm(m) for m in d["Page"]["media"]]


async def popular(client, page: int = 1, per: int = 24) -> list[dict]:
    d = await _query(client, PAGE_Q, {"page": page, "perPage": per, "sort": ["POPULARITY_DESC"]})
    return [_norm(m) for m in d["Page"]["media"]]


async def search(client, q: str, page: int = 1, per: int = 24) -> list[dict]:
    d = await _query(client, SEARCH_Q, {"page": page, "perPage": per, "search": q})
    return [_norm(m) for m in d["Page"]["media"]]


async def get_media(client, anime_id: int) -> dict | None:
    d = await _query(client, DETAIL_Q, {"id": int(anime_id)})
    return _norm(d.get("Media"))


# AniList's fixed genre vocabulary (used for the categories/browse UI).
GENRES = [
    "Action", "Adventure", "Comedy", "Drama", "Ecchi", "Fantasy", "Horror",
    "Mahou Shoujo", "Mecha", "Music", "Mystery", "Psychological", "Romance",
    "Sci-Fi", "Slice of Life", "Sports", "Supernatural", "Thriller",
]

BROWSE_Q = f"""
query ($page:Int,$perPage:Int,$sort:[MediaSort],$genre:String,$status:MediaStatus,$format:MediaFormat) {{
  Page(page:$page, perPage:$perPage) {{
    pageInfo {{ currentPage hasNextPage total }}
    media(type: ANIME, sort:$sort, genre:$genre, status:$status, format:$format, isAdult:false) {{ {MEDIA_FIELDS} }}
  }}
}}"""

FORMATS = {"MOVIE", "TV", "OVA", "ONA", "SPECIAL", "TV_SHORT"}


async def browse(client, genre=None, sort=None, status=None, fmt=None, page=1, per=24) -> list[dict]:
    variables = {"page": page, "perPage": per, "sort": sort or ["POPULARITY_DESC"]}
    if genre:
        variables["genre"] = genre
    if status:
        variables["status"] = status
    if fmt:
        variables["format"] = fmt
    d = await _query(client, BROWSE_Q, variables)
    return [_norm(m) for m in d["Page"]["media"]]


async def airing(client, page: int = 1, per: int = 24) -> list[dict]:
    return await browse(client, status="RELEASING", sort=["TRENDING_DESC"], page=page, per=per)
