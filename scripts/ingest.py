"""Ingest the AniList catalog -> MongoDB + Elasticsearch.

Standalone CLI — NOT imported by the running backend. Run from the backend root
(where the `app` package lives) so `from app import ...` resolves:

  python -m scripts.ingest popular [pages]    # top N popular (pages*50, max 100p = 5000)
  python -m scripts.ingest years [from] [to]  # full catalog via startDate-year slicing
  python -m scripts.ingest full               # popular sweep + year sweep (default)
  python -m scripts.ingest enrich [limit]     # per-anime heavy fields (relations/chars/...)
  python -m scripts.ingest sample [n]         # full parse of top-n (for testing)

Coverage: partitions by startDate.year (populated even when `season` is null, so
season-less OVA/movie/special are still captured), formats TV/TV_SHORT/MOVIE/OVA/
ONA/SPECIAL (MUSIC excluded), SFW only.

Dedup is automatic & free: every row upserts by _id (AniList id), so the
popularity and per-year sweeps union with no manual dedup, and re-running is
idempotent/resumable. AniList caps offset pagination at 5000 entries and runs
~30 req/min (degraded), so each year stays under the cap and we pace at ~2.2s.
"""
import asyncio
import datetime
import sys

import httpx
from pymongo import UpdateOne

from app import anilist, es
from app.db import ensure_indexes, get_db

PER_PAGE = 50
SLEEP = 2.2          # ~27 req/min, under AniList's degraded 30/min cap
MAX_POP_PAGES = 100  # 100 * 50 = 5000 = AniList's hard offset cap
FIRST_YEAR = 1940


async def _save(db, items: list[dict]) -> int:
    items = [a for a in items if a]
    if not items:
        return 0
    if db is not None:
        await db.anime.bulk_write(
            [UpdateOne({"_id": a["id"]}, {"$set": {k: v for k, v in a.items() if k != "id"}}, upsert=True)
             for a in items],
            ordered=False,
        )
    await es.index_anime(items)
    return len(items)


async def _retry(factory, what: str):
    """Call an async factory with backoff; None if it keeps failing."""
    for attempt in range(5):
        try:
            return await factory()
        except Exception as e:  # noqa: BLE001
            wait = 12 * (attempt + 1)
            print(f"  {what}: {e}; retry in {wait}s", flush=True)
            await asyncio.sleep(wait)
    print(f"  {what}: giving up", flush=True)
    return None


async def sweep_popular(client, db, max_pages: int) -> int:
    total = 0
    for page in range(1, min(max_pages, MAX_POP_PAGES) + 1):
        res = await _retry(lambda p=page: anilist.crawl_popular(client, p, PER_PAGE), f"[popular] p{page}")
        if res is None:
            break
        items, has_next = res
        if not items:
            break
        total += await _save(db, items)
        print(f"[popular] p{page} +{len(items)} total={total}", flush=True)
        await asyncio.sleep(SLEEP)
        if not has_next:
            break
    return total


async def sweep_years(client, db, y0: int, y1: int) -> int:
    total = 0
    for year in range(y0, y1 + 1):
        page, yc = 1, 0
        while True:
            res = await _retry(
                lambda p=page, y=year: anilist.crawl_year(client, y, p, PER_PAGE),
                f"[year {year}] p{page}",
            )
            if res is None:
                break
            items, has_next = res
            yc += await _save(db, items)
            await asyncio.sleep(SLEEP)
            if len(items) < PER_PAGE or not has_next:
                break
            page += 1
        total += yc
        if yc:
            print(f"[year {year}] +{yc} running_total={total}", flush=True)
    return total


HEAVY_FIELDS = ("relations", "characters", "staff", "recommendations", "reviews", "stats")


async def enrich_details(client, db, limit: int | None = None) -> int:
    """Per-anime detail pass: fetch relations/characters/staff/reviews/recs/stats
    and merge into each Mongo doc. One query per anime (can't batch — too heavy),
    so it's slow; run it after the bulk crawl, or scope with `limit`. Only touches
    docs that aren't already enriched, so it's safe to re-run as a catch-up cron.
    """
    if db is None:
        print("no mongo; nothing to enrich", flush=True)
        return 0
    cur = db.anime.find({"characters": {"$exists": False}}, {"_id": 1}).sort("popularity", -1)
    if limit:
        cur = cur.limit(limit)
    ids = [d["_id"] async for d in cur]
    print(f"[enrich] {len(ids)} anime to enrich", flush=True)
    n = 0
    for aid in ids:
        full = await _retry(lambda a=aid: anilist.get_media(client, a), f"[detail {aid}]")
        if full:
            extra = {k: full[k] for k in HEAVY_FIELDS if k in full}
            if extra:
                await db.anime.update_one({"_id": aid}, {"$set": extra})
                n += 1
        await asyncio.sleep(SLEEP)
        if n and n % 20 == 0:
            print(f"[enrich] {n}/{len(ids)}", flush=True)
    print(f"[enrich] done: {n} enriched", flush=True)
    return n


async def parse_full(client, db, ids: list[int]) -> int:
    """Full per-anime parse: fetch the complete detail doc (bulk + heavy fields)
    and upsert it. Used for small test samples so every field is populated."""
    n = 0
    for aid in ids:
        full = await _retry(lambda a=aid: anilist.get_media(client, a), f"[full {aid}]")
        if not full:
            continue
        if db is not None:
            await db.anime.update_one({"_id": full["id"]},
                                      {"$set": {k: v for k, v in full.items() if k != "id"}}, upsert=True)
        await es.index_anime([full])
        n += 1
        print(f"[full] {full['id']} {full['title']}: "
              f"relations={len(full.get('relations') or [])} "
              f"characters={len(full.get('characters') or [])} "
              f"staff={len(full.get('staff') or [])} "
              f"reviews={len(full.get('reviews') or [])} "
              f"recs={len(full.get('recommendations') or [])} "
              f"stats={'y' if full.get('stats') else 'n'}", flush=True)
        await asyncio.sleep(SLEEP)
    return n


async def run(mode: str = "full", *args) -> int:
    db = get_db()
    await ensure_indexes()
    await es.ensure_index()
    grand = 0
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            if mode in ("popular", "full"):
                pages = int(args[0]) if (mode == "popular" and args) else MAX_POP_PAGES
                grand += await sweep_popular(client, db, pages)
            if mode in ("years", "full"):
                now = datetime.datetime.utcnow().year
                y0 = int(args[0]) if (mode == "years" and len(args) > 0) else FIRST_YEAR
                y1 = int(args[1]) if (mode == "years" and len(args) > 1) else now + 1
                grand += await sweep_years(client, db, y0, y1)
            if mode == "enrich":
                limit = int(args[0]) if args else None
                grand += await enrich_details(client, db, limit)
            if mode == "sample":
                n = int(args[0]) if args else 5
                items, _ = await anilist.crawl_popular(client, 1, max(n, 5))
                grand += await parse_full(client, db, [a["id"] for a in items][:n])
    finally:
        await es.close()
    print(f"DONE ({mode}): {grand} upserts (re-counts overlaps; unique = db count)", flush=True)
    return grand


if __name__ == "__main__":
    asyncio.run(run(*(sys.argv[1:] or ["full"])))
