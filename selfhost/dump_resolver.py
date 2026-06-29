#!/usr/bin/env python3
"""dump_resolver.py — OFFLINE episode resolver over the AnimeTosho DB dump
(SQLite `at_index.sqlite`, built from torrents-latest by build_index.py).

Replaces the live, 429-limited `ingest.find_releases` for the back-catalog:
  AniList id -> AniDB aid/eid (ingest.map_anidb) -> local SQLite query ->
  best release per episode, PREFERRING NZB (seeder-independent) over torrent.

Emits todo items the farm consumes: {aid, ep, eid, source, url, name, size, title}
where source is "nzb" (url = constructed storage.animetosho.org NZB) or "torrent"
(url = magnet). The dump is frozen 2026-05-08 -> complete for back-catalog; use the
live feed for newer airing episodes.

Usage: dump_resolver.py --search "Title" [...] | --anilist ID [...] [--out file] [--allow-hevc 1]
"""
import sys, os, json, sqlite3, argparse, urllib.parse, re, time
sys.path.insert(0, "/data"); import ingest

DB = os.getenv("AT_INDEX", "/data/at_index.sqlite")
AL = "https://graphql.anilist.co"

def _con():
    c = sqlite3.connect(DB); c.row_factory = sqlite3.Row; return c

def nzb_url(tid, name):
    # Verified format: /nzbs/<id hex, 8-pad>/<URL-encoded release name>.nzb
    return f"https://storage.animetosho.org/nzbs/{tid:08x}/{urllib.parse.quote(name)}.nzb"

def _res(t):
    m = re.search(r"(2160|1080|720|480)p?", t); return int(m.group(1)) if m else 0

def _richness(name, allow_hevc):
    sc = {2160: 900, 1080: 1000, 720: 600, 480: 300}.get(_res(name), 0)
    if ingest.is_dualaudio(name): sc += 400          # dub
    if ingest.is_multisub(name):  sc += 200          # many subs
    if not ingest.is_hevc(name):  sc += 150          # H.264 -> instant remux
    return sc

def _plausible(sz, res, eps):
    """Is this release's size consistent with a REAL single episode (or movie) at this resolution?
    The only per-release 'is it actually the anime' signal the dump gives is SIZE — so reject:
      - too SMALL  -> promos/commercials/CMs/previews (e.g. a 20MB "Schick x Evangelion" ad), and
      - too BIG    -> multi-episode/whole-series packs mis-tagged to one eid (e.g. 59GB "1-26").
    Floors scale with resolution; movies (<=2 eps) are ~3x longer so get higher floor + ceiling."""
    sz = sz or 0
    movie = (eps or 0) <= 2
    lo = {1080: 0.40e9, 720: 0.20e9, 480: 0.08e9}.get(res, 0.05e9) * (3 if movie else 1)
    hi = 12e9 if movie else 8e9
    return lo <= sz <= hi

def _fmt_score(name):
    """Source FORMAT preference, used ONLY as a tiebreaker below english-sub/dub/many-subs.
    We re-encode the rungs to 8-bit 4:2:0 ~3Mbps + 128k AAC and losslessly remux only an
    8-bit H.264 native, so the IDEAL source is standard 8-bit H.264 4:2:0 with non-lossless
    audio: it remuxes cleanly (best native quality, no re-encode, no NVENC 10-bit/444 failures)
    AND downloads smallest (download is the farm's bottleneck). A 10-bit/4:4:4/HEVC/FLAC source
    is re-encoded anyway — its extra fidelity is discarded — and bloats the download, so it ranks
    lower. Video format dominates audio. Names don't always tag depth; absence => assume 8-bit."""
    n = (name or "").lower()
    is_hevc = any(t in n for t in ("x265", "hevc", "h265", "h.265"))
    is_10bit = any(t in n for t in ("10bit", "10-bit", "hi10", "yuv420p10", "yuv444p10")) or "p10" in n
    is_444 = "444" in n
    is_flac = "flac" in n
    vid = 0 if is_hevc else (1 if (is_10bit or is_444) else 2)   # 8bit-h264-420 best, h264-other mid, hevc worst
    aud = 0 if is_flac else 1                                    # non-lossless audio => smaller download
    return vid * 2 + aud

def _size_pref(sz, eps):
    """Prefer the SANE size band: too small = over-compressed trash, too big = bloat (e.g. 9GB
    FLAC) that wastes download/storage/ship (we Y-mode remux, so source size == served 1080p size).
    TV episode sweet spot ~0.8-3.8GB; movies (<=2 eps) tolerate up to ~12GB. Higher tuple = preferred."""
    sz = sz or 0
    hi = 12_000_000_000 if (eps or 0) <= 2 else 3_800_000_000
    lo = 800_000_000 if (eps or 0) > 2 else 1_500_000_000
    if lo <= sz <= hi: return (2, sz)     # in band -> best; larger-in-band = better quality
    if sz > hi:        return (1, -sz)    # bloat -> only if nothing in band; smaller-bloat first
    return (0, sz)                        # too small (trash) -> last resort; larger first

def resolve_anime(anilist_id, allow_hevc=True, con=None):
    """-> (items, mp). items = one dict per resolvable episode (NZB-first)."""
    own = con is None
    if own: con = _con()
    cur = con.cursor()
    try:
        mp = ingest.map_anidb(anilist_id)
    except Exception:
        mp = {}
    if not mp.get("anidb_id"):
        if own: con.close()
        return [], mp
    anidb = mp["anidb_id"]; e2 = {v: k for k, v in mp["eid_to_ep"].items()}
    eps = mp.get("episodes") or 0
    title = mp.get("english") or mp.get("romaji") or str(anilist_id)
    items = []; batch_needed = []
    for ep in range(1, eps + 1):
        eid = e2.get(ep)
        if not eid: continue
        rows = cur.execute(
            "SELECT t.id,t.name,t.magnet,t.btih,t.totalsize,t.stored_nzb,"
            "COALESCE(s.has_eng,0) AS has_eng, COALESCE(s.n_langs,0) AS n_langs "
            "FROM torrents t LEFT JOIN tsubs s ON s.torrent_id=t.id "
            "WHERE t.aid=? AND t.eid=? AND t.deleted=0", (anidb, eid)).fetchall()
        rows = [r for r in rows if r["name"] and _res(r["name"]) >= 480
                and (allow_hevc or not ingest.is_hevc(r["name"]))
                and _plausible(r["totalsize"] or 0, _res(r["name"]), eps)]   # real-episode size gate
        if not rows:
            batch_needed.append(ep); continue          # no plausible single -> try a batch pack
        # rank: resolution (top) -> a COMBINED quality score (English sub + English dub + many
        # subtitle langs, all weighted TOGETHER) -> sane size -> NZB(seeder-free) -> size.
        # English sub is weighted above English dub, so on a sub-vs-dub tie we keep the sub.
        # (has_eng / n_langs come from REAL track metadata via the attachments dump, not the name.)
        def _rank(r):
            nm = r["name"]
            res = {2160: 3, 1080: 4, 720: 2, 480: 1}.get(_res(nm), 0)
            quality = ((100 if r["has_eng"] else 0)                       # English subtitle (highest)
                       + (70 if ingest.is_dualaudio(nm) else 0)          # English dub (just under sub)
                       + 2 * min(r["n_langs"], 30))                      # many subtitle languages
            tier, within = _size_pref(r["totalsize"] or 0, eps)
            # res > track-quality (eng/dub/langs — never sacrificed) > size-tier (avoid bloat/trash)
            # > FORMAT (prefer lean remuxable 8-bit H.264, tiebreak only) > NZB > larger-in-band.
            return (res, quality, tier, _fmt_score(nm), 1 if r["stored_nzb"] == 1 else 0, within)
        rows.sort(key=_rank, reverse=True)
        b = rows[0]
        src = "nzb" if b["stored_nzb"] == 1 else "torrent"
        url = nzb_url(b["id"], b["name"]) if src == "nzb" else b["magnet"]
        # carry the magnet + btih too: torrent is the FALLBACK when the NZB has dead articles
        # (a 2nd Usenet backbone doesn't help — non-Omicron backbones carry 0% of AnimeTosho).
        item = {"aid": anilist_id, "ep": ep, "eid": eid, "source": src, "url": url,
                "magnet": b["magnet"], "btih": b["btih"], "name": b["name"],
                "size": b["totalsize"], "title": title,
                "has_eng_sub": bool(b["has_eng"]), "n_subs": b["n_langs"]}
        if src == "torrent":
            # RELIABILITY fallback: best NZB for this episode (even BLOATED, up to 12GB) so a dead
            # torrent still yields the episode via Usenet. The farm full-RE-ENCODES it (not remux) so
            # the bloat is squeezed out to ~1-1.5GB — "never miss an episode" without the storage waste.
            nb = cur.execute("SELECT id,name,totalsize FROM torrents WHERE aid=? AND eid=? AND deleted=0 "
                             "AND stored_nzb=1 AND (totalsize IS NULL OR totalsize<=12000000000)",
                             (anidb, eid)).fetchall()
            nb = [n for n in nb if n["name"] and _res(n["name"]) >= 480
                  and (allow_hevc or not ingest.is_hevc(n["name"]))]
            if nb:
                nb.sort(key=lambda n: _richness(n["name"], allow_hevc), reverse=True)
                bn = nb[0]
                item["nzb_fallback"] = nzb_url(bn["id"], bn["name"])
                item["nzb_fallback_name"] = bn["name"]; item["nzb_fallback_size"] = bn["totalsize"]
        items.append(item)
    if batch_needed:                                       # batch-only episodes -> one batch item
        brows = cur.execute(
            "SELECT id,name,magnet,btih,totalsize FROM torrents "
            "WHERE aid=? AND (labels&1)=1 AND deleted=0 AND magnet LIKE 'magnet%'", (anidb,)).fetchall()
        brows = [r for r in brows if r["name"] and _res(r["name"]) >= 480
                 and (allow_hevc or not ingest.is_hevc(r["name"]))]
        if brows:
            brows.sort(key=lambda r: (_richness(r["name"], allow_hevc), r["totalsize"]), reverse=True)
            bb = brows[0]
            items.append({"aid": anilist_id, "source": "batch", "url": bb["magnet"], "btih": bb["btih"],
                          "name": bb["name"], "season": mp.get("season", 1),
                          "eps": batch_needed, "title": title})
    if own: con.close()
    return items, mp

def _season_of_title(title):
    """Best-effort season number from a release title — catches S0N / 'Season N' / 'Nth Season'
    AND the Code Geass 'R2' = season-2 idiom that the shared parser misses. None = untagged
    (single-season shows are often untagged, so None is treated as 'matches')."""
    t = (title or "").lower()
    if re.search(r"\br2\b", t):
        return 2
    m = re.search(r"\bs(?:eason)?\s*0?(\d)\b", t) or re.search(r"\b(\d)(?:nd|rd|th)\s*season\b", t)
    return int(m.group(1)) if m else None

def _is_full_season_pack(title, eps):
    """A complete-season pack that map_batch's _is_batch misses: a season tag (S01) with NO
    single-episode marker, or an explicit 1..eps range, or a completeness keyword."""
    t = (title or "").lower()
    if re.search(r"\b(complete|batch|boxset|box\s*set|bd\s*box|all\s*episodes|collection)\b", t):
        return True
    if eps and re.search(r"\b0?1\s*[-~]\s*0?%d\b" % eps, t):
        return True
    # a season tag but NOT a single-episode title (no SxxEyy, '- 05', 'ep 5')
    if re.search(r"\bs0?\d\b|\bseason\s*\d\b", t) and not re.search(r"\bs\d+e\d+\b|\s-\s*\d+\b|\bep\s*\d+\b", t):
        return True
    return False

def resolve_anime_live(anilist_id, allow_hevc=True):
    """LIVE recovery resolver — for back-catalog whose FROZEN-dump torrents are stale/dead.
    Resolves via ingest.find_releases (current AnimeTosho API + Nyaa RSS, with LIVE seeder
    data) and the tested seeder-aware select_release, instead of the offline SQLite dump.
    Emits the SAME todo shape the farm consumes (source 'torrent'/'batch'). Proven: classics
    like Code Geass/Eva/Nausicaa are alive on Nyaa (75-200 seeders) though dead in the dump."""
    try:
        mp = ingest.map_anidb(anilist_id)
    except Exception:
        mp = {}
    if not mp.get("anidb_id"):
        return [], mp
    eps = mp.get("episodes") or 0
    title = mp.get("english") or mp.get("romaji") or str(anilist_id)
    ep2eid = {v: k for k, v in (mp.get("eid_to_ep") or {}).items()}
    eids = [ep2eid[e] for e in range(1, eps + 1) if e in ep2eid][:80] or None
    rels = ingest.find_releases(mp["anidb_id"], mp.get("romaji", ""), mp.get("english", ""), eids=eids)

    def _seed(r):
        s = r.get("seeders")
        try: return int(s) if str(s) not in ("None", "") else 0
        except (TypeError, ValueError): return 0

    # 1) PREFER well-seeded COMPLETE packs: one alive torrent yields many episodes. Classics are
    #    dead per-episode but have 40-80-seeder complete BD packs. Score = alive (seeders, saturated
    #    at 40 since past that download speed is gated elsewhere) + leaner per-ep + remuxable H.264.
    season = mp.get("season") or 1
    cands = []
    for r in rels:
        t = r.get("title", "")
        st = _season_of_title(t)
        if st is not None and st != season:
            continue                                           # wrong season (e.g. R2 pack for S1) -> reject
        br = ingest.map_batch(r, mp)
        if not br and _is_full_season_pack(t, eps) and st in (None, season):
            br = (1, eps)                                       # complete-season pack map_batch missed (no range/keyword)
        if not br:
            continue
        lo, hi = br; n = max(hi - lo + 1, 1)
        sd = _seed(r); per_mb = ingest._rel_mb(r) / n
        url = r.get("torrent_url") or r.get("magnet")
        if not url or sd < 5 or per_mb > 3000:                 # need peers + encode-friendly size (not huge BD)
            continue
        if re.search(r"(?i)\bav1\b", t):                       # AV1 has no HW decode on our GPUs -> ~40min encodes
            continue
        if not allow_hevc and ingest.is_hevc(r.get("title", "")):
            continue
        score = min(sd, 40) * 50 - int(per_mb / 100) + (30 if not ingest.is_hevc(r.get("title", "")) else 0)
        cands.append((score, lo, hi, url, r))
    cands.sort(reverse=True)

    covered = {}; chosen = {}                                   # ep -> url ; url -> [rel, [eps]]
    for score, lo, hi, url, r in cands:
        newly = [e for e in range(max(lo, 1), min(hi, eps) + 1) if e not in covered]
        if not newly:
            continue
        for e in newly: covered[e] = url
        chosen.setdefault(url, [r, []])[1].extend(newly)

    items = []
    for url, (r, beps) in chosen.items():
        items.append({"aid": anilist_id, "source": "batch", "url": url, "btih": r.get("info_hash"),
                      "name": r.get("title"), "season": mp.get("season", 1), "eps": sorted(beps),
                      "title": title, "live": True, "seeders": r.get("seeders")})

    # 2) singles (or per-ep batches) for episodes no alive pack covered
    sbatch = {}
    for ep in range(1, eps + 1):
        if ep in covered:
            continue
        sel, conf = ingest.select_release(rels, ep, mp, allow_hevc=allow_hevc)
        if not sel:
            continue
        url = sel.get("torrent_url") or sel.get("magnet")
        if not url:
            continue
        if conf == "batch":
            sbatch.setdefault(url, {"rel": sel, "eps": []})["eps"].append(ep)
        else:
            items.append({"aid": anilist_id, "ep": ep, "eid": ep2eid.get(ep), "source": "torrent",
                          "url": url, "magnet": sel.get("magnet") or url, "btih": sel.get("info_hash"),
                          "name": sel.get("title"), "size": sel.get("total_size") or 0, "title": title,
                          "seeders": sel.get("seeders"), "has_eng_sub": True, "n_subs": 0, "live": True})
    for url, b in sbatch.items():
        r = b["rel"]
        items.append({"aid": anilist_id, "source": "batch", "url": url, "btih": r.get("info_hash"),
                      "name": r.get("title"), "season": mp.get("season", 1),
                      "eps": sorted(b["eps"]), "title": title, "live": True})
    return items, mp

def search_id(title):
    q = "query($s:String){Media(search:$s,type:ANIME){id title{romaji english}}}"
    try:
        return ingest.hj(AL, json.dumps({"query": q, "variables": {"s": title}}).encode())["data"]["Media"]["id"]
    except Exception:
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="/data/todo_dump.jsonl")
    ap.add_argument("--anilist", nargs="*", type=int, default=[])
    ap.add_argument("--search", nargs="*", default=[])
    ap.add_argument("--top", type=int, default=0, help="resolve the top-N most popular AniList anime")
    ap.add_argument("--titles-file", default="", help="file of anime titles (one per line) to resolve")
    ap.add_argument("--allow-hevc", type=int, default=1)
    ap.add_argument("--live", action="store_true",
                    help="resolve via LIVE find_releases (current seeders) instead of the frozen "
                         "dump — recovery path for back-catalog whose dump torrents are dead")
    a = ap.parse_args()
    ids = list(a.anilist)
    if a.titles_file:
        for line in open(a.titles_file):
            t = line.strip()
            if not t or t.startswith("#"): continue
            i = search_id(t)
            if i: ids.append(i)
            time.sleep(0.6)
    if a.top:
        q = "query($p:Int,$pp:Int){Page(page:$p,perPage:$pp){media(type:ANIME,sort:POPULARITY_DESC){id}}}"
        page = 1
        while len(ids) < a.top:
            data = ingest.hj(AL, json.dumps({"query": q, "variables": {"p": page, "pp": 50}}).encode())
            chunk = [m["id"] for m in data["data"]["Page"]["media"]]
            if not chunk: break
            ids.extend(chunk); page += 1; time.sleep(1.0)
        ids = ids[:a.top]
    for t in a.search:
        i = search_id(t)
        if i: ids.append(i)
        time.sleep(0.6)
    con = None if a.live else _con(); n_eps = n_nzb = 0
    with open(a.out, "w") as f:
        for aid in ids:
            items, mp = (resolve_anime_live(aid, bool(a.allow_hevc)) if a.live
                         else resolve_anime(aid, bool(a.allow_hevc), con))
            for it in items:
                f.write(json.dumps(it) + "\n")
            nz = sum(1 for i in items if i.get("source") == "nzb")
            tor = sum(1 for i in items if i.get("source") == "torrent")
            bat = [i for i in items if i.get("source") == "batch"]
            bat_eps = sum(len(i["eps"]) for i in bat)
            n_eps += nz + tor; n_nzb += nz
            t = (mp.get("english") or mp.get("romaji") or str(aid)) if mp else str(aid)
            print(f"  [{t[:30]:30} id{aid}] single {nz+tor:>3} (nzb {nz}) | batch {len(bat)} pack(s) -> {bat_eps} eps", flush=True)
            time.sleep(0.3)
    if con: con.close()
    print(f"[dump_resolve] singles {n_eps} ({n_nzb} nzb) + batch packs -> {a.out}", flush=True)

if __name__ == "__main__":
    main()
