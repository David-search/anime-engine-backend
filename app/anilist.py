"""AniList GraphQL client — the catalog source of truth ('what anime exists').

We only read public metadata here; this layer is fully legitimate.

Two field tiers:
  MEDIA_FIELDS  — scalar/light data, pulled in the BULK crawl (every doc).
  DETAIL_EXTRA  — heavy relational data (characters/staff/reviews/...), pulled
                  only on the per-anime detail query (lazy, cached on view).
"""
from __future__ import annotations

import httpx

ANILIST_URL = "https://graphql.anilist.co"

# Formats we treat as "anime". MUSIC excluded (music videos: huge volume, noise).
ANIME_FORMATS = ["TV", "TV_SHORT", "MOVIE", "OVA", "ONA", "SPECIAL"]

MEDIA_FIELDS = """
id
idMal
title { romaji english native }
synonyms
coverImage { extraLarge large }
bannerImage
description(asHtml: false)
episodes
duration
format
status
source
season
startDate { year month day }
endDate { year month day }
genres
tags { name rank isMediaSpoiler }
averageScore
popularity
favourites
countryOfOrigin
isAdult
trailer { id site thumbnail }
studios { edges { isMain node { id name } } }
nextAiringEpisode { episode airingAt }
"""

# Heavy relational data — detail query only (would blow AniList's complexity
# budget at 50/page). Each node keeps its AniList id so it links to its own page.
DETAIL_EXTRA = """
relations {
  edges {
    relationType
    node { id type format episodes seasonYear title { romaji english } coverImage { large } }
  }
}
characters(sort: ROLE, perPage: 12) {
  edges {
    role
    node { id name { full } image { large } }
    voiceActors(language: JAPANESE) { id name { full } image { large } language: languageV2 }
  }
}
staff(perPage: 8) {
  edges { role node { id name { full } image { large } } }
}
recommendations(sort: RATING_DESC, perPage: 12) {
  nodes { mediaRecommendation { id title { romaji english } coverImage { large } } }
}
reviews(sort: RATING_DESC, perPage: 5) {
  nodes { id summary score user { id name avatar { large } } }
}
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
query ($id:Int) {{ Media(id:$id, type:ANIME) {{ {MEDIA_FIELDS} {DETAIL_EXTRA} }} }}"""

POP_Q = f"""
query ($page:Int,$perPage:Int,$formats:[MediaFormat]) {{
  Page(page:$page, perPage:$perPage) {{
    pageInfo {{ hasNextPage }}
    media(type: ANIME, sort:POPULARITY_DESC, format_in:$formats, isAdult:false) {{ {MEDIA_FIELDS} }}
  }}
}}"""

YEAR_Q = f"""
query ($page:Int,$perPage:Int,$gt:FuzzyDateInt,$lt:FuzzyDateInt,$formats:[MediaFormat]) {{
  Page(page:$page, perPage:$perPage) {{
    pageInfo {{ hasNextPage }}
    media(type: ANIME, sort:ID, startDate_greater:$gt, startDate_lesser:$lt,
          format_in:$formats, isAdult:false) {{ {MEDIA_FIELDS} }}
  }}
}}"""


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


def _date(d: dict | None) -> dict | None:
    d = d or {}
    if not d.get("year"):
        return None
    return {"year": d.get("year"), "month": d.get("month"), "day": d.get("day")}


def _norm_relations(rel: dict | None) -> list[dict]:
    out = []
    for e in (rel or {}).get("edges") or []:
        n = e.get("node") or {}
        nt = n.get("title") or {}
        nci = n.get("coverImage") or {}
        out.append({
            "id": n.get("id"), "type": n.get("type"), "format": n.get("format"),
            "relation": e.get("relationType"),
            "title": nt.get("english") or nt.get("romaji"),
            "poster": nci.get("large"), "year": n.get("seasonYear"),
            "episodes": n.get("episodes"),
        })
    return out


def _norm_characters(ch: dict | None) -> list[dict]:
    out = []
    for e in (ch or {}).get("edges") or []:
        n = e.get("node") or {}
        vas = [{
            "id": v.get("id"),
            "name": (v.get("name") or {}).get("full"),
            "image": (v.get("image") or {}).get("large"),
            "language": v.get("language"),
        } for v in (e.get("voiceActors") or [])]
        out.append({
            "id": n.get("id"),
            "name": (n.get("name") or {}).get("full"),
            "image": (n.get("image") or {}).get("large"),
            "role": e.get("role"),
            "voiceActors": vas,
        })
    return out


def _norm_staff(st: dict | None) -> list[dict]:
    out = []
    for e in (st or {}).get("edges") or []:
        n = e.get("node") or {}
        out.append({
            "id": n.get("id"),
            "name": (n.get("name") or {}).get("full"),
            "image": (n.get("image") or {}).get("large"),
            "role": e.get("role"),
        })
    return out


def _norm_recs(rec: dict | None) -> list[dict]:
    out = []
    for nd in (rec or {}).get("nodes") or []:
        mr = nd.get("mediaRecommendation") or {}
        if not mr:
            continue
        mt = mr.get("title") or {}
        out.append({
            "id": mr.get("id"),
            "title": mt.get("english") or mt.get("romaji"),
            "poster": (mr.get("coverImage") or {}).get("large"),
        })
    return out


def _norm_reviews(rv: dict | None) -> list[dict]:
    out = []
    for n in (rv or {}).get("nodes") or []:
        u = n.get("user") or {}
        out.append({
            "id": n.get("id"), "summary": n.get("summary"), "score": n.get("score"),
            "user": u.get("name"), "avatar": (u.get("avatar") or {}).get("large"),
        })
    return out


def _norm(m: dict | None) -> dict | None:
    if not m:
        return None
    t = m.get("title") or {}
    ci = m.get("coverImage") or {}
    nae = m.get("nextAiringEpisode")
    tr = m.get("trailer")

    studio_edges = (m.get("studios") or {}).get("edges") or []
    studios = [e["node"]["name"] for e in studio_edges if e.get("isMain") and e.get("node")]
    producers = [e["node"]["name"] for e in studio_edges if not e.get("isMain") and e.get("node")]

    tags = [{"name": x.get("name"), "rank": x.get("rank"), "spoiler": x.get("isMediaSpoiler")}
            for x in (m.get("tags") or [])]

    trailer = None
    if tr and tr.get("id"):
        tid = str(tr["id"]).strip()  # AniList sometimes embeds a stray tab in the id
        site = tr.get("site")
        if site == "youtube":
            url = f"https://www.youtube.com/watch?v={tid}"
            thumb = f"https://i.ytimg.com/vi/{tid}/hqdefault.jpg"  # rebuild (AniList's is dirty)
        elif site == "dailymotion":
            url = f"https://www.dailymotion.com/video/{tid}"
            thumb = (tr.get("thumbnail") or "").strip() or None
        else:
            url, thumb = None, (tr.get("thumbnail") or "").strip() or None
        trailer = {"id": tid, "site": site, "thumbnail": thumb, "url": url}

    out = {
        "id": m["id"],
        "idMal": m.get("idMal"),
        "title": t.get("english") or t.get("romaji") or t.get("native") or "Untitled",
        "titleRomaji": t.get("romaji"),
        "titleNative": t.get("native"),
        "synonyms": m.get("synonyms") or [],
        "popularity": m.get("popularity") or 0,
        "favourites": m.get("favourites") or 0,
        "description": m.get("description"),
        "poster": ci.get("extraLarge") or ci.get("large"),
        "banner": m.get("bannerImage"),
        "episodes": m.get("episodes"),
        "duration": m.get("duration"),
        "format": m.get("format"),
        "status": m.get("status"),
        "source": m.get("source"),
        "season": m.get("season"),
        "startDate": _date(m.get("startDate")),
        "endDate": _date(m.get("endDate")),
        "genres": m.get("genres") or [],
        "tags": tags,
        "score": m.get("averageScore"),
        "countryOfOrigin": m.get("countryOfOrigin"),
        "isAdult": m.get("isAdult"),
        "studios": studios,
        "producers": producers,
        "trailer": trailer,
        "nextAiring": {"episode": nae["episode"], "airingAt": nae["airingAt"]} if nae else None,
    }
    # Heavy fields only when the detail query actually fetched them, so a later
    # bulk sweep's $set never clobbers cached relations/characters/etc.
    if m.get("relations") is not None:
        out["relations"] = _norm_relations(m["relations"])
    if m.get("characters") is not None:
        out["characters"] = _norm_characters(m["characters"])
    if m.get("staff") is not None:
        out["staff"] = _norm_staff(m["staff"])
    if m.get("recommendations") is not None:
        out["recommendations"] = _norm_recs(m["recommendations"])
    if m.get("reviews") is not None:
        out["reviews"] = _norm_reviews(m["reviews"])
    return out


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


# --- ingest crawl helpers: each returns (normalized_items, has_next_page) ---

async def crawl_popular(client, page: int, per: int = 50) -> tuple[list[dict], bool]:
    d = await _query(client, POP_Q, {"page": page, "perPage": per, "formats": ANIME_FORMATS})
    p = d["Page"]
    return [_norm(m) for m in p["media"]], p["pageInfo"]["hasNextPage"]


async def crawl_year(client, year: int, page: int, per: int = 50) -> tuple[list[dict], bool]:
    d = await _query(client, YEAR_Q, {
        "page": page, "perPage": per,
        "gt": year * 10000 - 1, "lt": (year + 1) * 10000,
        "formats": ANIME_FORMATS,
    })
    p = d["Page"]
    return [_norm(m) for m in p["media"]], p["pageInfo"]["hasNextPage"]


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
