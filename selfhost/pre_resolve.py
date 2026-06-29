#!/usr/bin/env python3
"""
pre_resolve.py — paced, per-ANIME resolve of the build-farm scope into a static
`episode -> torrent-URL` list (JSONL). Run ONCE, offline. The farm then downloads
+ encodes straight from the list with NO mapping/AnimeTosho API call in the hot
loop — which is the actual bottleneck (per-episode resolve rate-limits, 429).

Per-ANIME (one map + one find_releases per title, all episodes at once) instead of
per-episode => ~15x fewer API calls => no 429, and the whole catalog resolves in
~minutes instead of days.

Scope: currently-airing (RELEASING) + top-popularity, deduped, skipping >100 eps.

Usage: pre_resolve.py [--out /data/todo.jsonl] [--airing 60] [--top 500]
                      [--pace 2.0] [--limit N]
"""
import sys, os, json, time, argparse, threading
sys.path.insert(0, "/data")
import ingest

AL = "https://graphql.anilist.co"

def _al(query, variables):
    return ingest.hj(AL, json.dumps({"query": query, "variables": variables}).encode())

def anime_scope(airing_n, top_n):
    out = {}
    q_air = ("query($n:Int){Page(perPage:$n){media(type:ANIME,status:RELEASING,sort:TRENDING_DESC){"
             "id episodes title{romaji english}}}}")
    for m in _al(q_air, {"n": airing_n})["data"]["Page"]["media"]:
        out[m["id"]] = m
    per = 50
    for page in range(1, max(1, top_n // per) + 1):
        q_pop = ("query($p:Int,$pp:Int){Page(page:$p,perPage:$pp){media(type:ANIME,sort:POPULARITY_DESC){"
                 "id episodes title{romaji english}}}}")
        for m in _al(q_pop, {"p": page, "pp": per})["data"]["Page"]["media"]:
            out.setdefault(m["id"], m)
        time.sleep(1.5)
    return [m for m in out.values() if 0 < (m.get("episodes") or 0) <= 100]

def resolve_anime(aid):
    """One map + one find_releases for the WHOLE anime -> per-episode torrent URLs."""
    mp = ingest.map_anidb(aid)
    if not mp.get("anidb_id"):
        return []
    eps = mp.get("episodes") or 0
    ep2eid = {v: k for k, v in mp["eid_to_ep"].items()}
    eids = [ep2eid[e] for e in range(1, eps + 1) if e in ep2eid][:120] or None
    rels = ingest.find_releases(mp["anidb_id"], mp["romaji"], mp["english"], eids=eids)
    out = []
    for ep in range(1, eps + 1):
        sel, conf = ingest.select_release(rels, ep, mp, allow_hevc=False)
        if sel and conf != "batch":
            out.append({"aid": aid, "ep": ep, "title": sel["title"],
                        "url": sel["torrent_url"], "conf": conf})
    return out

def resolve_anime_timed(aid, timeout):
    """resolve_anime with a hard per-anime wall-clock cap (some titles page AnimeTosho
    slowly / stall); returns "TIMEOUT" if exceeded so the caller can skip it."""
    res = [None]; done = threading.Event()
    def _run():
        try: res[0] = resolve_anime(aid)
        except Exception: res[0] = None
        finally: done.set()
    threading.Thread(target=_run, daemon=True).start()
    return res[0] if done.wait(timeout) else "TIMEOUT"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="/data/todo.jsonl")
    ap.add_argument("--airing", type=int, default=60)
    ap.add_argument("--top", type=int, default=500)
    ap.add_argument("--pace", type=float, default=2.0)   # seconds between anime (avoid 429)
    ap.add_argument("--limit", type=int, default=0)      # cap anime count (0 = all)
    ap.add_argument("--timeout", type=float, default=120)  # per-anime resolve cap (s)
    a = ap.parse_args()

    scope = anime_scope(a.airing, a.top)
    if a.limit:
        scope = scope[:a.limit]
    print(f"[pre_resolve] {len(scope)} anime in scope -> {a.out}", flush=True)
    n_eps = n_ani = 0
    with open(a.out, "w") as f:
        for i, m in enumerate(scope):
            aid = m["id"]
            title = (m["title"].get("english") or m["title"].get("romaji") or str(aid))
            try:
                eps = resolve_anime_timed(aid, a.timeout)
            except Exception as e:  # noqa: BLE001
                print(f"  [skip] {aid} {title[:40]}: {e}", flush=True); time.sleep(a.pace); continue
            if eps == "TIMEOUT":
                print(f"  [timeout] {aid} {title[:40]} (>{a.timeout:.0f}s, skipped)", flush=True); time.sleep(a.pace); continue
            eps = eps or []
            for e in eps:
                f.write(json.dumps(e) + "\n")
            f.flush()
            n_eps += len(eps); n_ani += 1 if eps else 0
            print(f"  [{i+1}/{len(scope)}] {title[:44]:44} -> {len(eps):>3} eps (total {n_eps})", flush=True)
            time.sleep(a.pace)
    print(f"[pre_resolve] DONE: {n_eps} episodes from {n_ani} anime -> {a.out}", flush=True)

if __name__ == "__main__":
    main()
