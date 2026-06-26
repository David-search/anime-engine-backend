#!/usr/bin/env python3
"""
ingest.py — headless video library-filler for the AniChan VIDEO-ORIGIN tier.

Runs ON the video node (NOT the clean backend). Pipeline per episode:
  AniList id --(ani.zip)--> AniDB id --(AnimeTosho aid query)--> structured
  per-episode releases --> select best per sourcing policy --> download via
  transmission --> hls_build.py (remux master + NVENC ladder + all subs/audio)
  --> register in cache_db (the index nginx-served HLS is tracked by).

Sourcing policy: prefer 1080p H.264 MultiSub single-episode (one download =
instant remux master + every subtitle language). Falls back HEVC / lower res.

CLI:
  ingest.py episode <anilist_id> <ep> [--category sub] [--dry-run] [--no-hevc]
  ingest.py series  <anilist_id> [--eps 1-12|1,3,5] [--dry-run] [--no-hevc]
  ingest.py stats | evict <cap_gb> | reindex      (delegates to cache_db)
"""
import sys, os, re, json, time, subprocess, argparse, urllib.request, urllib.parse, urllib.error, datetime
import xml.etree.ElementTree as ET, email.utils
import cache_db
import relparser as rp   # ported Amatsu parser (season-aware episode extraction)

LIBRARY = "/data/library"
CACHE = "/data/cache"
HLS_BUILD = "/data/hls_build.py"
UA = "anichan-ingest/1.0"
BACKEND_URL = os.getenv("BACKEND_URL", "").rstrip("/")   # backend to push cache-state to
INGEST_TOKEN = os.getenv("INGEST_TOKEN", "")             # shared secret (== backend SELFHOST_INGEST_TOKEN)

# ---------- mapping + discovery ----------
def hj(url, data=None, retries=4):
    """GET/POST JSON with retry on transient errors (AniList 429/500 are common)."""
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, data=data, headers={
                "Content-Type": "application/json", "Accept": "application/json", "User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            last = e
            if e.code in (429, 500, 502, 503, 504) and i < retries - 1:
                time.sleep(2 * (i + 1)); continue
            raise
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            last = e
            if i < retries - 1:
                time.sleep(2 * (i + 1)); continue
            raise
    raise last

def _map_anidb_live(anilist_id):
    q = "query($id:Int){Media(id:$id,type:ANIME){id episodes synonyms title{romaji english native}}}"
    al = hj("https://graphql.anilist.co",
            json.dumps({"query": q, "variables": {"id": anilist_id}}).encode())
    m = al["data"]["Media"]
    az = hj(f"https://api.ani.zip/mappings?anilist_id={anilist_id}")
    eps = az.get("episodes", {}) or {}
    # Authoritative maps from ani.zip, all keyed to OUR requested episode number —
    # which is the ani.zip dict KEY `k` (it equals the AniList episode you ask for).
    # For a split cour the season-relative `episodeNumber` differs from `k`
    # (Dr. Stone Cour 3 ep "1" has episodeNumber 25): keying on `k` is what lets a
    # cour-relative request reach the right release.
    #   eid_to_ep:    AniDB episode id            -> our ep   (parser-free join)
    #   abs_to_ep:    fansub absolute number      -> our ep
    #   relnum_to_ep: (TVDB season, season ep#)   -> our ep   (S04E25 -> 1)
    eid_to_ep, abs_to_ep, ep_airdate, relnum_to_ep, ep_titles = {}, {}, {}, {}, {}
    season = None

    def _ek(kv):
        try: return int(kv[0])
        except (ValueError, TypeError): return 10**9   # non-numeric specials sort last
    # Iterate in numeric episode order with FIRST-WINS (setdefault), so ani.zip
    # duplicate rows can't clobber the real mapping. (199221's finale row is
    # mislabeled episodeNumber=25/absolute=83 — same as ep1 — which last-write-wins
    # would map S04E25 to ep 13 instead of ep 1 for un-eid'd releases.)
    for k, v in sorted(eps.items(), key=_ek):
        try:
            ours = int(k)            # the ani.zip key == AniList/requested episode #
        except (ValueError, TypeError):
            continue
        if ours < 1:
            continue  # skip specials (ep 0 / S-entries)
        if v.get("anidbEid"):
            try: eid_to_ep.setdefault(int(v["anidbEid"]), ours)
            except (ValueError, TypeError): pass
        if v.get("absoluteEpisodeNumber"):
            try: abs_to_ep.setdefault(int(v["absoluteEpisodeNumber"]), ours)
            except (ValueError, TypeError): pass
        sn, srel = v.get("seasonNumber"), v.get("episodeNumber")
        if sn is not None and srel is not None:
            try: relnum_to_ep.setdefault((int(sn), int(srel)), ours)
            except (ValueError, TypeError): pass
        ad = v.get("airdate") or v.get("airDate") or v.get("airDateUtc")
        if ad:
            ep_airdate.setdefault(ours, ad)
        ti = v.get("title") or {}                       # ani.zip episode titles (TVDB/AniDB)
        title = ti.get("en") or ti.get("x-jat") or ti.get("ja")
        if title and ours not in ep_titles:             # first (non-dup) row wins
            ep_titles[ours] = title
        if season is None:
            season = v.get("seasonNumber")
    romaji = m["title"]["romaji"] or ""
    english = m["title"]["english"] or ""
    az_count = len([e for e in eps if str(e).isdigit() and int(e) >= 1])
    # the entry's own cour/part number (from its title) — used to reject releases
    # of a DIFFERENT cour ("Part 2 - 06" when we want Part 3).
    part = rp.explicit_part(english) or rp.explicit_part(romaji)
    return {"anilist_id": anilist_id,
            "anidb_id": az.get("mappings", {}).get("anidb_id"),
            "episodes": m.get("episodes") or az_count or None,
            "season": season, "part": part,
            "title": english or romaji, "romaji": romaji, "english": english,
            "synonyms": [s for s in (m.get("synonyms") or []) if s],
            "eid_to_ep": eid_to_ep, "abs_to_ep": abs_to_ep,
            "relnum_to_ep": relnum_to_ep, "ep_airdate": ep_airdate, "ep_titles": ep_titles}

# ---- mapping persistence (cache the ani.zip→AniDB map; survive outages) ----
MAP_TTL = 12 * 3600   # refresh a covered mapping after this; stale cache still used if ani.zip is down

def _serialize_map(mp):
    d = dict(mp)
    d["relnum_list"] = [[s, e, v] for (s, e), v in (mp.get("relnum_to_ep") or {}).items()]
    d.pop("relnum_to_ep", None)
    return json.dumps(d)

def _deserialize_map(s):
    d = json.loads(s)
    for k in ("eid_to_ep", "abs_to_ep", "ep_airdate", "ep_titles"):
        d[k] = {int(kk): vv for kk, vv in (d.get(k) or {}).items()}   # JSON stringifies int keys
    d["relnum_to_ep"] = {(int(a), int(b)): int(v) for a, b, v in d.pop("relnum_list", [])}
    return d

def _covered(mp, want_ep):
    """Does the cached map already know how to resolve `want_ep`? (i.e. not a
    just-aired episode that postdates the cache)."""
    if want_ep is None:
        return True
    known = (set((mp.get("eid_to_ep") or {}).values()) | set((mp.get("abs_to_ep") or {}).values())
             | set((mp.get("relnum_to_ep") or {}).values()))
    if want_ep in known:
        return True
    if not known:                       # ani.zip had no per-episode rows -> trust the count
        n = mp.get("episodes") or 0
        return bool(n) and 1 <= want_ep <= n
    return False

def map_anidb(anilist_id, want_ep=None):
    """Mapping with a node-side cache: use the stored ani.zip map when it already
    covers `want_ep` and is within TTL; otherwise refetch live and persist. If the
    live fetch fails (ani.zip/AniList down), fall back to the stale cached map so
    re-ingesting a known anime still works."""
    row = cache_db.mapping_get(anilist_id)
    cached = None
    if row:
        try:
            cached = _deserialize_map(row[0])
        except Exception:  # noqa: BLE001
            cached = None
    if cached is not None and _covered(cached, want_ep) and (time.time() - row[1]) < MAP_TTL:
        return cached
    try:
        live = _map_anidb_live(anilist_id)
        cache_db.mapping_put(anilist_id, _serialize_map(live))
        return live
    except Exception as e:  # noqa: BLE001
        if cached is not None:
            print(f"[map] live fetch failed ({e}); using cached mapping for {anilist_id}")
            return cached
        raise

# discovery: AnimeTosho's aid query only lists releases it auto-mapped to an AniDB
# episode (often a subset). A paginated title q-search fills the gaps; we dedup by
# torrent_url and reject q-search false positives by requiring >=2 distinctive
# title tokens to match. (This is the doc-02 "taming the mess" problem.)
_STOP = {"the", "season", "and", "of", "wa", "no", "ni", "o", "a", "to", "ova", "tv"}

def _tokens(*titles):
    toks = set()
    for t in titles:
        for w in re.findall(r"[A-Za-z0-9]{4,}", (t or "").lower()):
            if w not in _STOP:
                toks.add(w)
    return toks

def _relevant(title, toks):
    tl = title.lower()
    return sum(1 for w in toks if w in tl) >= 2

def _clean_q(title):
    # AnimeTosho full-text search is stricter with punctuation / season suffixes;
    # search the distinctive core words only (matches far more releases).
    t = re.sub(r"[^\w\s]", " ", title)
    t = re.sub(r"\b\d*(st|nd|rd|th)?\s*season\s*\d*\b", " ", t, flags=re.I)
    return re.sub(r"\s+", " ", t).strip()

_NYAA_NS = {"nyaa": "https://nyaa.si/xmlns/nyaa"}

def fetch_nyaa(query, pages=2, cat="1_2"):
    """Nyaa.si RSS — completeness fallback for just-aired episodes AnimeTosho
    hasn't indexed yet. Returns AnimeTosho-shaped dicts (anidb_eid=None, so they
    go through the season+airdate-guarded title mapping)."""
    out = []
    q = urllib.parse.quote(query)
    for p in range(1, pages + 1):
        url = f"https://nyaa.si/?page=rss&c={cat}&q={q}&s=seeders&o=desc&p={p}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=25) as r:
                root = ET.fromstring(r.read())
        except (urllib.error.URLError, ET.ParseError, TimeoutError, ConnectionError):
            break
        items = root.findall(".//item")
        for it in items:
            seeders = it.findtext("nyaa:seeders", namespaces=_NYAA_NS) or "0"
            pub = it.findtext("pubDate")
            ts = None
            if pub:
                try: ts = int(email.utils.parsedate_to_datetime(pub).timestamp())
                except (TypeError, ValueError): ts = None
            out.append({
                "title": it.findtext("title") or "",
                "torrent_url": it.findtext("link") or "",          # .torrent URL
                "info_hash": (it.findtext("nyaa:infoHash", namespaces=_NYAA_NS) or "").lower(),
                "seeders": int(seeders) if seeders.isdigit() else 0,
                "total_size": it.findtext("nyaa:size", namespaces=_NYAA_NS),
                "timestamp": ts, "anidb_eid": None, "_source": "nyaa"})
        if len(items) < 50:
            break
    return out

def find_releases(anidb_id, romaji="", english="", max_pages=6, eids=None):
    """Tiered discovery: (0) AnimeTosho by exact AniDB EPISODE id (parser-free,
    authoritative — collapses S04E25 / 'Part 3 - 01' / 'S4 - 25' onto one id),
    (1) AnimeTosho aid, (2) AnimeTosho keyword, (3) Nyaa RSS. Dedup by info_hash."""
    seen, out = set(), []

    def key_of(e):
        ih = (e.get("info_hash") or "").lower()
        return ih if ih else e.get("torrent_url")

    def add(rels, trusted, toks=None, source="animetosho"):
        for e in rels:
            k = key_of(e)
            if not k or k in seen:
                continue
            if toks is not None and not _relevant(e.get("title", ""), toks):
                continue
            seen.add(k); e.setdefault("_trusted", trusted); e.setdefault("_source", source)
            out.append(e)

    def pages(url_for):
        for p in range(1, max_pages + 1):
            chunk = hj(url_for(p))
            yield chunk
            if not chunk or len(chunk) < 50:
                break

    for eid in (eids or []):  # Tier 0 — exact AniDB episode id (authoritative, parser-free)
        for chunk in pages(lambda p, eid=eid: f"https://feed.animetosho.org/json?eid={eid}&only_tor=1&page={p}"):
            add(chunk, True)
    if anidb_id:  # Tier 1 — AniDB-mapped (authoritative anidb_eid)
        for chunk in pages(lambda p: f"https://feed.animetosho.org/json?aid={anidb_id}&only_tor=1&page={p}"):
            add(chunk, True)
    toks = _tokens(romaji, english)
    queries = {_clean_q(t) for t in (romaji, english) if _clean_q(t)}
    for qstr in queries:  # Tier 2 — AnimeTosho keyword
        q = urllib.parse.quote(qstr)
        for chunk in pages(lambda p, q=q: f"https://feed.animetosho.org/json?q={q}&only_tor=1&page={p}"):
            add(chunk, False, toks)
    for qstr in queries:  # Tier 2b — batch packs (finished/old shows, long-running back-catalog)
        bq = urllib.parse.quote(qstr + " batch")
        for chunk in pages(lambda p, bq=bq: f"https://feed.animetosho.org/json?q={bq}&only_tor=1&page={p}"):
            add(chunk, False, toks)
    for qstr in queries:  # Tier 3 — Nyaa RSS (just-aired / un-indexed) + Nyaa batches
        add(fetch_nyaa(qstr, pages=2), False, toks, source="nyaa")
        add(fetch_nyaa(qstr + " batch", pages=1), False, toks, source="nyaa")
    return out

# ---------- release parsing + selection ----------
_EP_RE = [re.compile(r"[Ss](\d{1,2})[Ee](\d{1,3})"),
          re.compile(r"\s-\s(\d{1,3})(?:v\d)?\s*[\[(]"),
          re.compile(r"\s-\s(\d{1,3})(?:v\d)?\s*$")]

def parse_episode(title):
    m = _EP_RE[0].search(title)
    if m:
        return int(m.group(2))
    for rgx in _EP_RE[1:]:
        m = rgx.search(title)
        if m:
            return int(m.group(1))
    return None

# Confidence of an episode determination, high -> low. Authoritative anidb_eid
# first; an airdate-confirmed TVDB-relabel is strong; bare title parse is weakest.
CONF_RANK = {"eid": 5, "sxxeyy": 4, "absolute": 3, "airdate": 2, "parsed": 1}
STRONG = {"eid", "sxxeyy", "absolute", "airdate"}   # acceptable for completeness

def _airdate_ok(ctx, ep, ts, window_days=45):
    """True if a release published at epoch `ts` is within window of AniList
    episode `ep`'s ani.zip airdate — confirms a TVDB-relabeled S01EXX is really
    this cour's episode (and not the prior season's same-numbered episode)."""
    if not ts:
        return False
    ad = (ctx.get("ep_airdate") or {}).get(ep)
    if not ad:
        return False
    try:
        d = datetime.datetime.strptime(str(ad)[:10], "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)
        rel_t = datetime.datetime.fromtimestamp(int(ts), datetime.timezone.utc)
        return abs((rel_t - d).days) <= window_days
    except (ValueError, TypeError, OverflowError, OSError):
        return False

def _is_batch(rel, season):
    """A multi-episode pack (explicit range or batch keyword). Conservative so a
    single episode is never misread as a batch."""
    t = rel.get("title", "")
    if rp.get_batch_range(t):
        return True
    return bool(re.search(
        r"\b(batch|complete|collection|boxset|box-?set|bd-?box|dvd-?box|all\s*episodes|all\s*eps)\b|全集|완결|전편",
        t, re.I))

def _relmap_has(relmap, s):
    """ani.zip provides a season-relative number for THIS season -> it is
    authoritative for it (a season number not in the map is a different cour)."""
    return s is not None and any(sn == s for (sn, _e) in relmap)

def _resolve_num(raw, ctx, es_tag=None):
    """A raw release number -> OUR episode, or None. Resolution order:
      1. season-relative via ani.zip relnum_to_ep (S04E25 -> our 1) — and when
         ani.zip has season-rel data for the season, it is AUTHORITATIVE: a number
         it doesn't know for that season is a different cour -> reject (no slop).
      2. fansub absolute (>N and in abs_to_ep).
      3. direct in-range number (single-cour shows w/o season-rel data).
    `es_tag` is the release's explicit season tag (else the anime's season)."""
    relmap = ctx.get("relnum_to_ep") or {}
    absmap = ctx["abs_to_ep"]
    n = ctx.get("episodes") or 0
    s = es_tag if es_tag is not None else ctx.get("season")
    if _relmap_has(relmap, s):
        if (s, raw) in relmap:
            return relmap[(s, raw)]                  # season-verified -> our ep
        if raw in absmap and (not n or raw > n):
            return absmap[raw]                       # fansub absolute count
        return None                                  # different cour -> reject
    if raw in absmap and (not n or raw > n):
        return absmap[raw]
    if 1 <= raw <= (n or raw):
        return raw
    return None

def map_batch(rel, ctx):
    """If rel is a batch, return (start_ep, end_ep) range of OUR episodes it covers,
    else None. Title-based — no download needed for coverage."""
    t = rel.get("title", "")
    season = ctx.get("season") or 1
    n = ctx.get("episodes") or 0
    ep_part, rel_part = ctx.get("part"), rp.explicit_part(t)
    if not _is_batch(rel, season) or rp.is_wrong_season(t, season):
        return None
    if ep_part and rel_part and rel_part != ep_part:
        return None                                  # a different cour's pack
    rng = rp.get_batch_range(t)
    if rng:
        if ep_part and rel_part == ep_part:          # our cour -> cour-relative range
            rs, re_ = rng["start"], rng["end"]
        else:
            rs, re_ = _resolve_num(rng["start"], ctx), _resolve_num(rng["end"], ctx)
        if rs and re_ and rs <= re_:
            return (rs, min(re_, n) if n else re_)
        return None
    if n:  # batch keyword, no explicit range -> whole season (season/cour-appropriate only)
        es = rp.explicit_season(t)
        if ((season == 1 and es in (None, 1)) or (season > 1 and es == season)
                or (ep_part and rel_part == ep_part)):
            return (1, n)
    return None

def map_episode(rel, ctx):
    """Return (episode_number, confidence) or (None, None). Authoritative
    anidb_eid first; else SEASON/COUR-VERIFIED title parse (ported Amatsu) +
    season-relative/absolute resolution. Rejects cross-season AND cross-cour slop
    (e.g. S04E06 'cour 1' for a cour-3 request) — the 'never the wrong ep' rule."""
    if _is_batch(rel, ctx.get("season") or 1):
        return None, None               # batch packs are resolved by map_batch
    eid = rel.get("anidb_eid")
    if eid not in (None, "", 0, "0"):
        try:
            eid = int(eid)
        except (ValueError, TypeError):
            eid = None
        if eid:
            if eid in ctx["eid_to_ep"]:
                return ctx["eid_to_ep"][eid], "eid"     # exact, zero parsing
            return None, None       # eid maps to a special / other cour/season -> reject
    t = rel.get("title", "")
    season = ctx.get("season") or 1
    n = ctx.get("episodes") or 0
    relmap = ctx.get("relnum_to_ep") or {}
    absmap = ctx["abs_to_ep"]
    ts = rel.get("timestamp")        # AnimeTosho publish epoch (None for some sources)

    # explicit SxxEyy. ani.zip's (season, season-ep) map is authoritative when it
    # has data for the embedded season — so 'S04E25' -> our 1 and 'S04E06' (a
    # different cour of the same season) is rejected, not mis-served as our ep 6.
    es, ee = rp.extract_sxxeyy(t)
    if es is not None:
        if (es, ee) in relmap:
            return relmap[(es, ee)], "sxxeyy"
        if _relmap_has(relmap, es):                         # season known, this ep isn't ours
            if ee in absmap and (not n or ee > n):
                return absmap[ee], "absolute"
            return None, None
        if es == season:                                    # no season-rel data: legacy path
            if 1 <= ee <= n:
                return ee, "sxxeyy"
            if ee in absmap:
                return absmap[ee], "absolute"
            return None, None
        if ee in absmap and (not n or ee > n):              # continuous absolute (S01E19 -> ep7)
            return absmap[ee], "absolute"
        if es == 1 and season != 1 and 1 <= ee <= n and _airdate_ok(ctx, ee, ts):
            return ee, "airdate"                            # TVDB-relabeled sequel, airdate-confirmed
        return None, None

    # no SxxEyy. Cour/part tags disambiguate the numbering scheme.
    if rp.is_wrong_season(t, season):
        return None, None
    ep_part, rel_part = ctx.get("part"), rp.explicit_part(t)
    if ep_part and rel_part and rel_part != ep_part:
        return None, None           # a DIFFERENT cour of the same season -> reject
    raw = rp.extract_episode_number(t, season)
    if raw is None or raw == -1:
        return None, None
    if ep_part and rel_part == ep_part:                     # 'Part 3 - 06' -> cour-relative
        return (raw, "sxxeyy") if 1 <= raw <= (n or raw) else (None, None)
    resolved = _resolve_num(raw, ctx, rp.explicit_season(t))  # 'S4 - 28' / '- 83' / '- 06'
    if resolved is None:
        return None, None
    if n and raw > n:
        return resolved, "absolute"
    if rp.explicit_season(t) == season:
        return resolved, "sxxeyy"
    return resolved, "parsed"

def coverage(releases, ctx):
    """Completeness gate. Each episode is obtainable via a STRONG single-file
    release or a batch pack (extract one file). Reports per-episode source:
    covered (strong single) / weak (parsed-only) / batch_only / gap."""
    n = ctx.get("episodes") or 0
    single = {}                 # ep -> [confidences] from single-file releases
    batched = set()             # eps covered by some batch pack
    for r in releases:
        ep, conf = map_episode(r, ctx)
        if ep:
            single.setdefault(ep, []).append(conf)
            continue
        br = map_batch(r, ctx)
        if br:
            for e in range(br[0], br[1] + 1):
                if 1 <= e <= (n or e):
                    batched.add(e)
    rng = list(range(1, n + 1)) if n else sorted(set(single) | batched)
    rep = {"episodes": n, "covered": [], "weak": [], "batch_only": [], "gaps": [],
           "batched_total": len(batched)}
    for ep in rng:
        confs = single.get(ep, [])
        if any(c in STRONG for c in confs):
            rep["covered"].append(ep)
        elif ep in batched:
            rep["batch_only"].append(ep)
        elif confs:
            rep["weak"].append(ep)
        else:
            rep["gaps"].append(ep)
    obtainable = set(rep["covered"]) | batched     # strong single OR a batch
    rep["complete"] = bool(n) and all(e in obtainable for e in range(1, n + 1))
    return rep

def res_of(t):
    m = re.search(r"(2160|1080|720|480)p", t)
    return int(m.group(1)) if m else 0

def is_hevc(t):   return bool(re.search(r"hevc|x265|x\.265|av1", t, re.I))
def is_multisub(t): return bool(re.search(r"multi.?sub", t, re.I))
def is_dualaudio(t):
    """A release carrying >1 audio (JP + a dub) — one download serves BOTH sub and
    dub. Title hints only; the real audio set is confirmed by ffprobe at build time."""
    return bool(re.search(r"dual.?audio|multi.?audio|dual.?lang|multi.?lang|\bdual\b|\bmulti\b", t, re.I))
def is_batch(t):
    return bool(re.search(r"\bbatch\b|\bvol\.?\b|\(\d{1,3}\s*[-~]\s*\d{1,3}\)|\[\d{1,3}\s*[-~]\s*\d{1,3}\]", t, re.I))

def score(rel, allow_hevc):
    t = rel["title"]
    s = int(rel.get("seeders") or 0)
    sc = {1080: 1000, 720: 600, 480: 300}.get(res_of(t), 0)
    if is_dualaudio(t):
        sc += 400                      # BOTH sub + dub in one download — first choice
    if is_multisub(t):
        sc += 200                      # we want every subtitle language
    if not is_hevc(t):
        sc += 150                      # H.264 -> instant remux master
    sc += min(s, 500) / 10             # seeders, capped so they don't dominate
    return sc

MIN_SEED = 1   # a torrent reporting fewer is treated as ~dead: last resort only

def _live(rel):
    """1 if the torrent has >= MIN_SEED seeders (or seeders unknown), else 0. A
    dead 1080p H.264 MultiSub would otherwise top the score and never download."""
    s = rel.get("seeders")
    if s is None:
        return 1            # unknown (some AnimeTosho rows omit it) -> don't penalize
    return 1 if int(s or 0) >= MIN_SEED else 0

def select_release(releases, ep, ctx, allow_hevc=True):
    """Pick the best release for episode `ep`. Prefer a single-file release that
    AUTHORITATIVELY maps to `ep`, ranked by (confidence, alive, quality) — so a
    dead torrent never beats a seeded one of equal confidence, but correctness
    always outranks seeds. Falls back to a batch pack covering `ep` (per-file).
    Returns (release, conf); conf == 'batch' means extract one file from the pack."""
    singles, batches = [], []
    for r in releases:
        t = r.get("title", "")
        if not r.get("torrent_url") or res_of(t) < 480:
            continue
        if not allow_hevc and is_hevc(t):
            continue
        epn, conf = map_episode(r, ctx)
        if epn == ep:
            # Correctness first as a STRONG-vs-weak gate (all of eid/sxxeyy/absolute/
            # airdate are episode-correct via the collision guards), THEN seeded, THEN
            # quality+dual-audio score, with eid as the final tiebreaker. This lets a
            # strong dual-audio release outrank an eid sub-only one (we want the dub),
            # without ever promoting a weak `parsed` guess.
            singles.append((1 if conf in STRONG else 0, _live(r), score(r, allow_hevc),
                            1 if conf == "eid" else 0, r, conf))
            continue
        br = map_batch(r, ctx)
        if br and br[0] <= ep <= br[1]:
            batches.append((_live(r), score(r, allow_hevc), r))
    if singles:
        singles.sort(key=lambda c: (c[0], c[1], c[2], c[3]), reverse=True)
        return singles[0][4], singles[0][5]
    if batches:
        batches.sort(key=lambda c: (c[0], c[1]), reverse=True)
        return batches[0][2], "batch"
    return None, None

def _size_mb(rel):
    s = rel.get("total_size")
    if isinstance(s, (int, float)):
        return "%dMB" % round(s / 1048576)            # AnimeTosho: bytes
    m = re.match(r"([\d.]+)\s*([KMGT])i?B", str(s or ""))   # Nyaa: "1.3 GiB"
    if m:
        mult = {"K": 1 / 1024, "M": 1, "G": 1024, "T": 1048576}[m.group(2)]
        return "%dMB" % round(float(m.group(1)) * mult)
    return "?"

def describe(rel):
    t = rel["title"]
    return (f"{t[:64]}  [{res_of(t)}p, {'HEVC' if is_hevc(t) else 'H.264'}, "
            f"{'MultiSub' if is_multisub(t) else 'sub'}, {rel.get('seeders')} seed, "
            f"{_size_mb(rel)}, src={rel.get('_source','animetosho')}]")

# ---------- transmission ----------
def tr(*args):
    return subprocess.run(["transmission-remote", *args], capture_output=True, text=True).stdout

def torrent_ids():
    ids = set()
    for line in tr("-l").splitlines():
        m = re.match(r"\s*(\d+)\s", line)
        if m:
            ids.add(int(m.group(1)))
    return ids

def download(torrent_url, timeout_s=1800, poll=10):
    before = torrent_ids()
    tr("-a", torrent_url, "--download-dir", LIBRARY)
    time.sleep(3)
    new = torrent_ids() - before
    tid = max(new) if new else (max(torrent_ids()) if torrent_ids() else None)
    if tid is None:
        raise RuntimeError("could not add torrent")
    tr("-t", str(tid), "-s")  # ensure started
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        info = tr("-t", str(tid), "-i")
        dm = re.search(r"Percent Done:\s*([\d.]+)%", info)
        st = re.search(r"State:\s*(.+)", info)
        pct = float(dm.group(1)) if dm else 0.0
        if pct >= 100 or (st and ("Finished" in st.group(1) or "Seeding" in st.group(1))):
            break
        time.sleep(poll)
    info = tr("-t", str(tid), "-i")
    name = re.search(r"Name:\s*(.+)", info)
    loc = re.search(r"Location:\s*(.+)", info)
    if not (name and loc):
        raise RuntimeError("torrent info missing name/location")
    path = os.path.join(loc.group(1).strip(), name.group(1).strip())
    if os.path.isdir(path):  # multi-file torrent -> biggest .mkv inside
        mkvs = [os.path.join(r, f) for r, _, fs in os.walk(path) for f in fs if f.lower().endswith(".mkv")]
        path = max(mkvs, key=os.path.getsize) if mkvs else path
    if not os.path.exists(path):
        raise RuntimeError(f"downloaded file not found at {path}")
    return tid, path

def _parse_size(s):
    m = re.match(r"([\d.]+)\s*([KMGT])?i?B", str(s).strip())
    if not m:
        return 0
    mult = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4, None: 1}[m.group(2)]
    return int(float(m.group(1)) * mult)

def _tr_files(tid):
    """Parse `transmission-remote -t <id> -f` into [{index,name,size}]."""
    files = []
    for line in tr("-t", str(tid), "-f").splitlines():
        m = re.match(r"\s*(\d+):\s+\d+%\s+\S+\s+\S+\s+([\d.]+\s*[KMGT]?i?B|None)\s+(.+)$", line)
        if m:
            files.append({"index": int(m.group(1)), "name": m.group(3).strip(),
                          "size": _parse_size(m.group(2))})
    return files

def download_batch_file(torrent_url, ep, season, timeout_s=3600, poll=10):
    """Add a batch pack, identify episode `ep`'s file (Amatsu select_best_video_file),
    and download ONLY that file (deselect the rest) — no full-pack download."""
    before = torrent_ids()
    tr("-a", torrent_url, "--download-dir", LIBRARY)
    time.sleep(3)
    new = torrent_ids() - before
    tid = max(new) if new else (max(torrent_ids()) if torrent_ids() else None)
    if tid is None:
        raise RuntimeError("could not add batch torrent")
    files, t0 = [], time.time()
    while time.time() - t0 < 180:            # .torrent metadata is immediate; get file list
        files = _tr_files(tid)
        if files:
            break
        time.sleep(2)
    if not files:
        raise RuntimeError("batch file list unavailable")
    pick = rp.select_best_video_file(files, ep, season)
    if not pick:
        raise RuntimeError(f"no file for ep{ep} in {len(files)}-file batch")
    # keep ONLY the picked file wanted — deselect the rest in ONE command so the
    # torrent never reaches the 0-wanted "complete" state (which global ratio-0 stops).
    others = [str(f["index"]) for f in files if f["index"] != pick["index"]]
    if others:
        tr("-t", str(tid), "-G", ",".join(others))
    tr("-t", str(tid), "-g", str(pick["index"]))
    tr("-t", str(tid), "-sr", "999")          # per-torrent ratio so global ratio-0 won't stop it
    tr("-t", str(tid), "-s")
    t0 = time.time()
    while time.time() - t0 < timeout_s:      # wait for the selected file only
        dm = re.search(r"Percent Done:\s*([\d.]+)%", tr("-t", str(tid), "-i"))
        if dm and float(dm.group(1)) >= 100:
            break
        time.sleep(poll)
    loc = re.search(r"Location:\s*(.+)", tr("-t", str(tid), "-i"))
    path = os.path.join(loc.group(1).strip() if loc else LIBRARY, pick["name"])
    if not os.path.exists(path):
        raise RuntimeError(f"extracted file not found: {path}")
    return tid, path

# ---------- build + register ----------
_nvenc_ok = None
def nvenc_available():
    """Probe NVENC once. Ephemeral GPU nodes can lose NVENC (driver/NVML version
    mismatch -> CUDA_ERROR_COMPAT_NOT_SUPPORTED); when that happens we must fall
    back to libx264 (CPU) or every build fails. Cached for the process lifetime."""
    global _nvenc_ok
    if _nvenc_ok is None:
        try:
            r = subprocess.run(["ffmpeg", "-hide_banner", "-f", "lavfi",
                                "-i", "nullsrc=s=128x128:d=0.1", "-c:v", "h264_nvenc", "-f", "null", "-"],
                               capture_output=True, timeout=30)
            _nvenc_ok = (r.returncode == 0)
        except Exception:  # noqa: BLE001
            _nvenc_ok = False
        if not _nvenc_ok:
            print("  [hls] NVENC unavailable -> CPU (libx264) encode")
    return _nvenc_ok

def build_and_register(file, anilist_id, ep, category, source_title):
    out = os.path.join(CACHE, str(anilist_id), str(ep), category)
    cmd = ["python3", HLS_BUILD, file, out, "--renditions", "1080,720,480"]
    if not nvenc_available():
        cmd.append("--no-nvenc")        # GPU lost -> CPU encode (the ladder still builds)
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"hls_build failed:\n{p.stdout[-1500:]}\n{p.stderr[-500:]}")
    rpt = json.loads(p.stdout[p.stdout.index("{"):])
    sub_langs = [s.get("lang") for s in rpt.get("subs", []) if s.get("converted")]
    rends = [f"{v['height']}p" for v in rpt.get("video", [])]
    cache_db.register(anilist_id, ep, category, out, renditions=rends,
                      audio_tracks=len(rpt.get("audio", [])), sub_langs=sub_langs,
                      source_title=source_title)
    return out, rpt, sub_langs, rends

def push_cache_state(anilist_id, mp):
    """Best-effort: tell the backend which episodes of this anime are now cached +
    their ani.zip titles, so the catalog can show coverage badges + episode titles
    without probing the origin. Never blocks or fails an ingest."""
    if not BACKEND_URL or not INGEST_TOKEN:
        return
    try:
        cached = {cat: cache_db.cached_eps(anilist_id, cat) for cat in ("sub", "dub")}
        payload = {"anilist_id": anilist_id, "total_eps": mp.get("episodes"), "cached": cached,
                   "ep_titles": {str(k): v for k, v in (mp.get("ep_titles") or {}).items()}}
        req = urllib.request.Request(
            f"{BACKEND_URL}/api/watch/cache-state", data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json", "X-Ingest-Token": INGEST_TOKEN, "User-Agent": UA})
        urllib.request.urlopen(req, timeout=8).read()
        print(f"  [cache-state] pushed {anilist_id}: sub{cached['sub']} dub{cached['dub']}")
    except Exception as e:  # noqa: BLE001
        print(f"  [cache-state] push skipped: {e}")

def ingest_one(anilist_id, ep, sel, conf, category, season, mp=None):
    print(f"  [select] ep{ep} (match={conf}): {describe(sel)}")
    if conf == "batch":
        tid, file = download_batch_file(sel["torrent_url"], ep, season)
    else:
        tid, file = download(sel["torrent_url"])
    print(f"  [dl]     {os.path.basename(file)}")
    out, rpt, sub_langs, rends = build_and_register(file, anilist_id, ep, category, sel["title"])
    print(f"  [done]   -> {out}  ({rpt['total_mb']}MB, rends {rends}, {len(sub_langs)} subs {sub_langs})")
    if mp:
        push_cache_state(anilist_id, mp)

# ---------- commands ----------
def cmd_episode(a):
    if cache_db.is_cached(a.anilist_id, a.ep, a.category):
        print(f"[skip] {a.anilist_id} ep{a.ep} already cached"); return
    mp = map_anidb(a.anilist_id, a.ep)
    print(f"[map] AniList {a.anilist_id} '{mp['title']}' -> AniDB {mp['anidb_id']} (eps={mp['episodes']})")
    if not mp["anidb_id"]:
        print("[err] no AniDB mapping"); return
    ep2eid = {v: k for k, v in mp["eid_to_ep"].items()}
    eids = [ep2eid[a.ep]] if a.ep in ep2eid else None
    rels = find_releases(mp["anidb_id"], mp["romaji"], mp["english"], eids=eids)
    sel, conf = select_release(rels, a.ep, mp, allow_hevc=not a.no_hevc)
    if not sel:
        print(f"[err] no release for ep{a.ep}"); return
    if a.dry_run:
        print(f"  [dry-run select] ep{a.ep} (match={conf}): {describe(sel)}"); return
    ingest_one(a.anilist_id, a.ep, sel, conf, a.category, mp.get("season") or 1, mp)

def parse_eps(spec, ep_count):
    if not spec:
        return list(range(1, (ep_count or 0) + 1))
    out = []
    for part in spec.split(","):
        if "-" in part:
            lo, hi = part.split("-"); out += list(range(int(lo), int(hi) + 1))
        else:
            out.append(int(part))
    return out

def _report_coverage(cov):
    obtainable = len(cov["covered"]) + len(cov["batch_only"])
    print(f"[coverage] {obtainable}/{cov['episodes']} obtainable "
          f"({len(cov['covered'])} single + {len(cov['batch_only'])} batch) | complete={cov['complete']}")
    if cov["gaps"]:        print(f"  [GAP]        no source at all: {cov['gaps'][:20]}")
    if cov["batch_only"]:  print(f"  [BATCH-ONLY] extract one file from a pack: {cov['batch_only'][:20]}")
    if cov["weak"]:        print(f"  [WEAK]       title-parsed only, no batch backup: {cov['weak'][:20]}")

def cmd_series(a):
    mp = map_anidb(a.anilist_id)
    print(f"[map] AniList {a.anilist_id} '{mp['title']}' -> AniDB {mp['anidb_id']} (eps={mp['episodes']})")
    if not mp["anidb_id"]:
        print("[err] no AniDB mapping"); return
    ep2eid = {v: k for k, v in mp["eid_to_ep"].items()}
    _want = parse_eps(a.eps, mp.get("episodes") or 0)
    eids = [ep2eid[e] for e in _want if e in ep2eid][:80] or None
    rels = find_releases(mp["anidb_id"], mp["romaji"], mp["english"], eids=eids)
    cov = coverage(rels, mp)
    _report_coverage(cov)
    if a.require_complete and not cov["complete"]:
        print("[abort] --require-complete: season is not fully covered; not ingesting."); return
    for ep in parse_eps(a.eps, mp.get("episodes") or (max(cov["covered"]) if cov["covered"] else 0)):
        if cache_db.is_cached(a.anilist_id, ep, a.category):
            print(f"[skip] ep{ep} cached"); continue
        sel, conf = select_release(rels, ep, mp, allow_hevc=not a.no_hevc)
        if not sel:
            print(f"[miss] ep{ep}: no release (single or batch)"); continue
        if a.dry_run:
            print(f"  [dry-run] ep{ep} (match={conf}): {describe(sel)}"); continue
        try:
            ingest_one(a.anilist_id, ep, sel, conf, a.category, mp.get("season") or 1, mp)
        except Exception as e:
            print(f"  [fail] ep{ep}: {e}")

def cmd_coverage(a):
    mp = map_anidb(a.anilist_id)
    print(f"[map] AniList {a.anilist_id} '{mp['title']}' -> AniDB {mp['anidb_id']} (eps={mp['episodes']})")
    if not mp["anidb_id"]:
        print("[err] no AniDB mapping"); return
    rels = find_releases(mp["anidb_id"], mp["romaji"], mp["english"])
    print(f"[discovery] {len(rels)} releases "
          f"({sum(1 for r in rels if r.get('_trusted'))} aid-mapped, "
          f"{sum(1 for r in rels if not r.get('_trusted'))} via title-search)")
    _report_coverage(coverage(rels, mp))

def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    pe = sub.add_parser("episode"); pe.add_argument("anilist_id", type=int); pe.add_argument("ep", type=int)
    pe.add_argument("--category", default="sub"); pe.add_argument("--dry-run", action="store_true")
    pe.add_argument("--no-hevc", action="store_true")
    ps = sub.add_parser("series"); ps.add_argument("anilist_id", type=int)
    ps.add_argument("--eps", default=None); ps.add_argument("--category", default="sub")
    ps.add_argument("--dry-run", action="store_true"); ps.add_argument("--no-hevc", action="store_true")
    ps.add_argument("--require-complete", action="store_true")
    pc = sub.add_parser("coverage"); pc.add_argument("anilist_id", type=int)
    pst = sub.add_parser("stats"); pev = sub.add_parser("evict"); pev.add_argument("cap_gb", type=float)
    pri = sub.add_parser("reindex")
    a = ap.parse_args()
    if a.cmd == "episode":  cmd_episode(a)
    elif a.cmd == "series": cmd_series(a)
    elif a.cmd == "coverage": cmd_coverage(a)
    elif a.cmd == "stats":  cache_db._print_stats(cache_db.DB_DEFAULT)
    elif a.cmd == "evict":
        res = cache_db.evict(a.cap_gb)
        print(json.dumps(res, indent=2))
        for aid in sorted({r["anilist_id"] for r in res.get("removed", [])}):
            try:
                push_cache_state(aid, map_anidb(aid))   # sync coverage to Mongo (cached map -> no ani.zip)
            except Exception as e:  # noqa: BLE001
                print(f"  [evict-sync] {aid}: {e}")
    elif a.cmd == "reindex": print("reindexed:", cache_db.reindex())

if __name__ == "__main__":
    main()
