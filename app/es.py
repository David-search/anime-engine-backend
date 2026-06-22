"""Elasticsearch accessor: index + autosuggest + full search.

No-op if ELASTIC_URL is unset. Index uses a `search_as_you_type` field for
typo/prefix autosuggestion across title + romaji + synonyms.
"""
from __future__ import annotations

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from .config import settings

_es: AsyncElasticsearch | None = None
INDEX = settings.ELASTIC_INDEX

MAPPING = {
    "mappings": {
        "properties": {
            "anilist": {"type": "integer"},
            "idMal": {"type": "integer"},
            "title": {"type": "text", "fields": {"kw": {"type": "keyword"}}},
            "titleRomaji": {"type": "text"},
            "titleNative": {"type": "text"},
            "synonyms": {"type": "text"},
            "suggest": {"type": "search_as_you_type"},
            "format": {"type": "keyword"},
            "status": {"type": "keyword"},
            "startDate": {"properties": {
                "year": {"type": "integer"}, "month": {"type": "integer"}, "day": {"type": "integer"},
            }},
            "score": {"type": "integer"},
            "genres": {"type": "keyword"},
            "tags": {"type": "text", "fields": {"kw": {"type": "keyword"}}},
            "source": {"type": "keyword"},
            "studios": {"type": "keyword"},
            "season": {"type": "keyword"},
            "meanScore": {"type": "integer"},
            "favourites": {"type": "long"},
            "episodes": {"type": "integer"},
            "popularity": {"type": "long"},
            "poster": {"type": "keyword", "index": False},
        }
    }
}


def get_es() -> AsyncElasticsearch | None:
    global _es
    if not settings.ELASTIC_URL:
        return None
    if _es is None:
        auth = (settings.ELASTIC_USER, settings.ELASTIC_PASSWORD) if settings.ELASTIC_PASSWORD else None
        _es = AsyncElasticsearch(hosts=[settings.ELASTIC_URL], basic_auth=auth, request_timeout=15)
    return _es


def _doc(a: dict) -> dict:
    syn = " ".join(a.get("synonyms") or [])
    suggest = " ".join(filter(None, [a.get("title"), a.get("titleRomaji"), a.get("titleNative"), syn]))
    return {
        "anilist": a["id"], "idMal": a.get("idMal"),
        "title": a.get("title"), "titleRomaji": a.get("titleRomaji"),
        "titleNative": a.get("titleNative"),
        "synonyms": syn, "suggest": suggest,
        "format": a.get("format"), "status": a.get("status"),
        "startDate": a.get("startDate"), "score": a.get("score"),
        "genres": a.get("genres") or [], "episodes": a.get("episodes"),
        "tags": [t.get("name") for t in (a.get("tags") or [])],
        "source": a.get("source"), "studios": a.get("studios") or [],
        "season": a.get("season"),
        "favourites": a.get("favourites") or 0,
        "popularity": a.get("popularity") or 0, "poster": a.get("poster"),
    }


async def ensure_index() -> None:
    es = get_es()
    if es is None:
        return
    if not await es.indices.exists(index=INDEX):
        await es.indices.create(index=INDEX, body=MAPPING)


async def index_anime(items: list[dict]) -> int:
    es = get_es()
    if es is None or not items:
        return 0
    actions = [{"_index": INDEX, "_id": a["id"], "_source": _doc(a)} for a in items]
    ok, _ = await async_bulk(es, actions, raise_on_error=False)
    return ok


def _hit_to_card(src: dict) -> dict:
    return {
        "id": src.get("anilist"), "idMal": src.get("idMal"),
        "title": src.get("title"), "titleRomaji": src.get("titleRomaji"),
        "titleNative": src.get("titleNative"), "poster": src.get("poster"),
        "format": src.get("format"), "startDate": src.get("startDate"),
        "score": src.get("score"), "episodes": src.get("episodes"),
        "genres": src.get("genres", []),
    }


async def suggest(q: str, size: int = 8) -> list[dict]:
    es = get_es()
    if es is None or not q.strip():
        return []
    body = {
        "size": size,
        "query": {"multi_match": {
            "query": q, "type": "bool_prefix",
            "fields": ["suggest", "suggest._2gram", "suggest._3gram"],
        }},
        "sort": ["_score", {"popularity": "desc"}],
    }
    r = await es.search(index=INDEX, body=body)
    return [_hit_to_card(h["_source"]) for h in r["hits"]["hits"]]


async def search(q: str, size: int = 40) -> list[dict]:
    es = get_es()
    if es is None or not q.strip():
        return []
    body = {
        "size": size,
        "query": {"multi_match": {
            "query": q, "type": "best_fields", "fuzziness": "AUTO",
            "fields": ["title^3", "titleRomaji^2", "titleNative^2", "synonyms", "tags"],
        }},
        "sort": ["_score", {"popularity": "desc"}],
    }
    r = await es.search(index=INDEX, body=body)
    return [_hit_to_card(h["_source"]) for h in r["hits"]["hits"]]


# Sort presets for the AniList-style filter UI.
_SORTS = {
    "POPULARITY_DESC": [{"popularity": "desc"}],
    "SCORE_DESC": [{"score": "desc"}, {"popularity": "desc"}],
    "TRENDING_DESC": [{"popularity": "desc"}],
    "START_DATE_DESC": [{"startDate.year": "desc"}, {"popularity": "desc"}],
    "TITLE": [{"title.kw": "asc"}],
}


async def filter_search(*, q: str = "", genres=None, tags=None, fmt=None, status=None,
                        season=None, source=None, year=None, sort="POPULARITY_DESC",
                        page: int = 1, size: int = 30) -> dict:
    """AniList-style faceted browse over ES. Genres/tags are AND-ed. Paginated."""
    es = get_es()
    if es is None:
        return {"results": [], "total": 0, "hasNext": False}
    must = []
    filt = []
    if (q or "").strip():
        must.append({"multi_match": {
            "query": q, "type": "best_fields", "fuzziness": "AUTO",
            "fields": ["title^3", "titleRomaji^2", "titleNative^2", "synonyms", "tags"],
        }})
    for g in (genres or []):
        filt.append({"term": {"genres": g}})
    for t in (tags or []):
        filt.append({"term": {"tags.kw": t}})
    if fmt:
        filt.append({"term": {"format": fmt}})
    if status:
        filt.append({"term": {"status": status}})
    if season:
        filt.append({"term": {"season": season}})
    if source:
        filt.append({"term": {"source": source}})
    if year:
        filt.append({"term": {"startDate.year": int(year)}})

    sort_clause = list(_SORTS.get(sort, _SORTS["POPULARITY_DESC"]))
    if (q or "").strip():
        sort_clause = ["_score"] + sort_clause
    frm = (max(page, 1) - 1) * size
    r = await es.search(index=INDEX, body={
        "from": frm, "size": size,
        "query": {"bool": {"must": must or [{"match_all": {}}], "filter": filt}},
        "sort": sort_clause, "track_total_hits": True,
    })
    hits = r["hits"]["hits"]
    total = r["hits"]["total"]["value"]
    return {"results": [_hit_to_card(h["_source"]) for h in hits],
            "total": total, "hasNext": frm + len(hits) < total}


async def facets() -> dict:
    """Distinct genres / years / formats for populating the filter UI."""
    es = get_es()
    if es is None:
        return {}
    r = await es.search(index=INDEX, body={"size": 0, "aggs": {
        "genres": {"terms": {"field": "genres", "size": 50}},
        "tags": {"terms": {"field": "tags.kw", "size": 120}},
        "years": {"terms": {"field": "startDate.year", "size": 120, "order": {"_key": "desc"}}},
        "formats": {"terms": {"field": "format", "size": 20}},
    }})
    a = r["aggregations"]
    return {
        "genres": [b["key"] for b in a["genres"]["buckets"]],
        "tags": [b["key"] for b in a["tags"]["buckets"]],
        "years": [b["key"] for b in a["years"]["buckets"] if b["key"]],
        "formats": [b["key"] for b in a["formats"]["buckets"]],
    }


async def close() -> None:
    global _es
    if _es is not None:
        await _es.close()
        _es = None
