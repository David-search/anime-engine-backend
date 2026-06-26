#!/usr/bin/env python3
"""
precache.py — proactive pre-cache worker (Phase 1, the biggest cold-start lever).

Pulls the currently-airing + trending anime slate so the popular catalog is
already self-hosted BEFORE anyone opens it — the pattern Netflix Open Connect and
every durable operator use (see 12-cold-start-and-instant-playback.md). Feeds the
bounded `ingest_api` queue, which dedups vs cached/in-flight, caps concurrency,
and LRU-evicts to the storage cap — so re-running is cheap and self-limiting.

Run once (cron) or with --loop. Env: INGEST_API, INGEST_TOKEN, PRECACHE_TOP_N,
PRECACHE_MAX_EPS, PRECACHE_INTERVAL.
"""
import json, urllib.request, time, os, argparse

INGEST_API = os.getenv("INGEST_API", "http://localhost:8001").rstrip("/")
TOKEN = os.getenv("INGEST_TOKEN", "")
TOP_N = int(os.getenv("PRECACHE_TOP_N", "20"))
MAX_EPS = int(os.getenv("PRECACHE_MAX_EPS", "12"))      # cap episodes pre-pulled per title

def hj(url, data=None, headers=None):
    h = {"User-Agent": "anichan-precache", "Accept": "application/json"}
    if data:
        h["Content-Type"] = "application/json"
    if headers:
        h.update(headers)
    with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=h), timeout=30) as r:
        return json.load(r)

def airing_slate(n):
    q = ("query($n:Int){Page(perPage:$n){media(type:ANIME,status:RELEASING,"
         "sort:TRENDING_DESC){id episodes nextAiringEpisode{episode} title{romaji english}}}}")
    d = hj("https://graphql.anilist.co", json.dumps({"query": q, "variables": {"n": n}}).encode())
    return d["data"]["Page"]["media"]

def aired_eps(m):
    nxt = (m.get("nextAiringEpisode") or {}).get("episode")
    return max(0, nxt - 1) if nxt else (m.get("episodes") or 0)

def enqueue(aid, ep):
    try:
        # precache=1 -> low-priority lane, so a real viewer's on-demand open always jumps ahead
        return hj(f"{INGEST_API}/ingest?anilist_id={aid}&ep={ep}&precache=1", headers={"X-Ingest-Token": TOKEN})
    except Exception as e:  # noqa: BLE001
        return {"err": str(e)}

def run_once():
    # DISABLED (2026-06-26): proactive pre-cache / downloads are turned off. Self-host
    # builds are now a manual / build-farm step — nothing should auto-trigger torrents
    # or encodes. The original loop is kept below (unreachable) for reference; delete
    # this guard to re-enable.
    print("[precache] DISABLED — auto pre-cache/downloads are off (manual builds only)")
    return
    try:
        slate = airing_slate(TOP_N)
    except Exception as e:  # noqa: BLE001
        print(f"[precache] AniList slate failed: {e}"); return
    print(f"[precache] {len(slate)} airing titles (top {TOP_N} by trending)")
    queued = warmed = rejected = 0
    for m in slate:
        aid = m["id"]
        aired = aired_eps(m)
        title = (m["title"].get("english") or m["title"].get("romaji") or str(aid))
        if not aired:
            continue
        # the LATEST aired episodes (the just-aired one is the demand spike) + ep1 as
        # the new-viewer entry point. Pulling the OLDEST episodes (1..N) would miss the
        # newest episode of an ONGOING show — exactly what most viewers open.
        eps = sorted(set([1] + list(range(max(1, aired - MAX_EPS + 1), aired + 1))))
        for ep in eps:
            r = enqueue(aid, ep)
            queued += len(r.get("started") or [])
            warmed += len(r.get("warmed") or [])
            rejected += 1 if (not r.get("started") and not r.get("warmed")) else 0
            time.sleep(0.25)   # gentle on AniList + the node
        print(f"  {title[:44]:44} aired={aired} -> eps {eps}")
    print(f"[precache] queued ~{queued} new · {warmed} already-warm · {rejected} dedup/full "
          f"— ingest_api drains the bounded queue + LRU-evicts to cap")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--loop", action="store_true")
    ap.add_argument("--interval", type=int, default=int(os.getenv("PRECACHE_INTERVAL", "1800")))
    a = ap.parse_args()
    run_once()
    while a.loop:
        time.sleep(a.interval)
        run_once()
