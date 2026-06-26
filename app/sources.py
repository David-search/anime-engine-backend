"""
Stream resolution via the Miruro aggregator "secure pipe", abstracted into
stable, ranked sources (source1..source7) keyed on the *host* — not Miruro's
rotating provider codenames (bonk/ally/pewe/… change; the decoded host does not).

Pipe protocol (verified live from the server):
    GET {base}/api/secure/pipe?e={base64url(json)}
    request envelope: {"path","method":"GET","query":{…},"body":null,"version":"0.1.0"}
    response: base64url -> gzip -> JSON
Two paths:
    path="episodes"  query={anilistId}                              -> {mappings, providers}
    path="sources"   query={episodeId, provider, category, anilistId} -> {streams, subtitles}

See claude/research/post-hianime-landscape-and-miruro.md for the full teardown.
"""
import asyncio
import base64
import gzip
import json
import re
import time
from typing import Any, Optional

import httpx

from .config import settings

# Base domains rotate; iterate in order (.online is dead from our IP, .bz works).
MIRURO_BASES = [
    "https://www.miruro.bz",
    "https://www.miruro.to",
    "https://www.miruro.tv",
    "https://www.miruro.ru",
]

# Final curated sources: ONLY hosts that reliably PLAY (measured over many titles),
# one source per host, via the path that works for it — "clean" = our ad-free
# player (proxied hls/mp4), "embed" = the host's own iframe (runs in the user's
# browser, so it dodges our proxy's flakiness). Order = display order: clean first,
# embeds last. Flat numbered list (source1..sourceN); no categories.
RELIABLE_SOURCES: list[tuple[str, str]] = [
    ("animedao", "clean"),   # most stable clean hls — promoted to source1 (least buffering)
    ("anidbapp", "clean"),   # 100% clean hls
    ("animegg",  "clean"),   # 100% clean mp4
    ("allmanga", "embed"),   # clean flaky -> embed (100%); biggest library + dub
    ("anikoto",  "embed"),   # HiAnime / megaplay -> embed (100%)
]
_CURATED_HOSTS = [h for h, _ in RELIABLE_SOURCES]
LABEL_TO_HOST = {f"source{i}": h for i, (h, _) in enumerate(RELIABLE_SOURCES, 1)}

# Embed servers removed entirely (region-locked / dead / too ad-heavy) — matched
# case-insensitively against the embed's server label or URL. The embed-server
# counterpart to RELIABLE_SOURCES; extend freely.
BLOCKED_EMBEDS = ["ok.ru"]   # ok.ru is region-locked to Russia (movieBlocked elsewhere)

_PIPE_HDRS = {
    "Accept": "*/*",
    "Origin": "https://www.miruro.to",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
}

# Global cap on concurrent Miruro calls across the whole app (live + ingest):
# never burst into a 429, even under load. Excess calls queue.
_PIPE_SEM = asyncio.Semaphore(4)


def _enc(payload: dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")


def _dec(text: str) -> Any:
    text += "=" * (-len(text) % 4)
    raw = base64.urlsafe_b64decode(text)
    try:
        raw = gzip.decompress(raw)
    except Exception:  # noqa: BLE001 — some payloads aren't gzipped
        pass
    return json.loads(raw.decode("utf-8", "replace"))


def _host_of(ep_id: str) -> str:
    """Episode id is base64 of '<host>:<slug>:<handle>'."""
    try:
        dec = base64.urlsafe_b64decode(ep_id + "=" * (-len(ep_id) % 4)).decode("utf-8", "replace")
        return dec.split(":", 1)[0]
    except Exception:  # noqa: BLE001
        return "?"


async def _pipe(http: httpx.AsyncClient, payload: dict) -> Optional[Any]:
    enc = _enc(payload)
    async with _PIPE_SEM:  # global concurrency cap — the main defense against bursting into 429
        for base in MIRURO_BASES:
            for attempt in range(2):  # one retry per base on rate-limit
                try:
                    r = await http.get(
                        f"{base}/api/secure/pipe",
                        params={"e": enc},
                        headers={**_PIPE_HDRS, "Referer": base + "/"},
                        timeout=20.0,
                    )
                    if r.status_code == 200 and r.text.strip():
                        return _dec(r.text.strip())
                    if r.status_code == 429:  # rate-limited: brief backoff, retry same base
                        await asyncio.sleep(1.5 * (attempt + 1))
                        continue
                    break  # other non-200 -> next base
                except Exception:  # noqa: BLE001 — next base
                    break
    return None


# --- tiny in-process TTL cache for the episodes map (durable, few KB each) ---
_ep_cache: dict[int, tuple[float, dict]] = {}
_EP_TTL = 600  # 10 minutes

# Resolved server lists cached briefly: a popular episode resolves ONCE per window
# (shared across all users) instead of ~7 Miruro calls per view — the biggest 429
# defense. Short TTL so the session-bound stream URLs stay fresh.
_servers_cache: dict[tuple, tuple[float, list]] = {}
_SERVERS_TTL = 180  # 3 minutes


async def _episodes_raw(http: httpx.AsyncClient, anilist_id: int) -> Optional[dict]:
    hit = _ep_cache.get(anilist_id)
    if hit and time.time() - hit[0] < _EP_TTL:
        return hit[1]
    data = await _pipe(
        http,
        {"path": "episodes", "method": "GET", "query": {"anilistId": anilist_id},
         "body": None, "version": "0.1.0"},
    )
    if isinstance(data, dict) and data.get("providers"):
        _ep_cache[anilist_id] = (time.time(), data)
    return data if isinstance(data, dict) else None


def _index_providers(data: dict) -> dict[str, dict]:
    """provider-codename -> {host, cats: {sub|dub: {ep_number: ep_id}}}"""
    out: dict[str, dict] = {}
    for prov, pd in (data.get("providers") or {}).items():
        if not isinstance(pd, dict):
            continue
        eps = pd.get("episodes") or {}
        host = "?"
        cats: dict[str, dict] = {}
        for cat in ("sub", "dub"):
            lst = eps.get(cat)
            if isinstance(lst, list) and lst:
                cats[cat] = {e.get("number"): e.get("id") for e in lst if e.get("id")}
                if host == "?":
                    host = _host_of(lst[0]["id"])
        if host != "?":
            out[prov] = {"host": host, "cats": cats}
    return out


async def list_sources(http: httpx.AsyncClient, anilist_id: int) -> dict:
    """Return the ranked, available sources for an anime + the max episode count."""
    data = await _episodes_raw(http, anilist_id)
    if not data:
        return {"episodes": 0, "sources": []}
    idx = _index_providers(data)
    by_host: dict[str, dict] = {}
    # availEps = number of AVAILABLE episodes. Use each provider's episode COUNT, not
    # the max episode NUMBER — a provider can emit concatenated/bogus numbers (animegg
    # returns 166167/147148/2526 for double-episodes), which max(number) inherits. Then
    # take the cross-provider CONSENSUS and reject wild outliers (a provider that
    # fuzzy-matched a DIFFERENT, longer anime — e.g. allmanga returning a 30-ep range
    # for a 1-ep movie) by dropping counts far above the median.
    counts = []
    for info in idx.values():
        by_host.setdefault(info["host"], info)
        c = max((len(m) for m in info["cats"].values()), default=0)
        if c:
            counts.append(c)
    maxep = 0
    if counts:
        counts.sort()
        med = counts[(len(counts) - 1) // 2]          # lower median = robust consensus
        maxep = max((c for c in counts if c <= med * 3), default=med)
    sources = []
    for i, (host, _mode) in enumerate(RELIABLE_SOURCES, 1):
        info = by_host.get(host)
        if not info:
            continue
        sources.append({
            "name": f"source{i}",
            "host": host,
            "sub": bool(info["cats"].get("sub")),
            "dub": bool(info["cats"].get("dub")),
        })
    return {"episodes": maxep, "sources": sources}


async def resolve(http: httpx.AsyncClient, anilist_id: int, source: str, ep: int, category: str) -> Optional[dict]:
    """Resolve one (source, episode, category) -> {type, url, referer, subtitles, intro, outro}."""
    want_host = LABEL_TO_HOST.get(source)
    if not want_host:
        return None
    data = await _episodes_raw(http, anilist_id)
    if not data:
        return None
    idx = _index_providers(data)

    prov_name = ep_id = None
    for prov, info in idx.items():
        if info["host"] != want_host:
            continue
        m = info["cats"].get(category) or {}
        if ep in m:
            prov_name, ep_id = prov, m[ep]
            break
    if not ep_id:
        return None

    src = await _pipe(
        http,
        {"path": "sources", "method": "GET",
         "query": {"episodeId": ep_id, "provider": prov_name, "category": category, "anilistId": anilist_id},
         "body": None, "version": "0.1.0"},
    )
    if not isinstance(src, dict):
        return None
    streams = src.get("streams") or []
    subs = src.get("subtitles") or []
    hls = next((s for s in streams if s.get("type") == "hls" and s.get("url")), None)
    if hls:
        return {"type": "hls", "url": hls["url"], "referer": hls.get("referer") or "",
                "subtitles": subs, "intro": src.get("intro"), "outro": src.get("outro")}
    emb = next((s for s in streams if s.get("type") == "embed" and s.get("url")), None)
    if emb:
        return {"type": "embed", "url": emb["url"], "referer": emb.get("referer") or "", "subtitles": subs}
    return None


def _embed_blocked(s: dict) -> bool:
    blob = f"{s.get('server') or ''} {s.get('url') or ''}".lower()
    return any(b in blob for b in BLOCKED_EMBEDS)


def _epref(s: dict) -> int:
    """Embed-server preference: the cluster's own players first (global, light ads),
    file-lockers after."""
    lbl = (s.get("server") or "").lower()
    if any(k in lbl for k in ("vidstream", "vidplay", "megaplay", "vidtube", "hd-", "kiwi", "uni")):
        return 0
    return 1


_LANG_NAMES = {
    "eng": "English", "jpn": "Japanese", "por": "Portuguese", "spa": "Spanish",
    "ara": "Arabic", "fre": "French", "fra": "French", "ger": "German", "deu": "German",
    "ita": "Italian", "rus": "Russian", "pol": "Polish", "dut": "Dutch", "nld": "Dutch",
    "tur": "Turkish", "kor": "Korean", "chi": "Chinese", "zho": "Chinese", "vie": "Vietnamese",
    "ind": "Indonesian", "tha": "Thai", "heb": "Hebrew", "ron": "Romanian", "ukr": "Ukrainian",
    "hun": "Hungarian", "ces": "Czech", "gre": "Greek", "ell": "Greek", "und": "Subtitle",
}

def _sub_label(lang: str, name: str) -> str:
    """Readable subtitle label: language name + a region hint parsed out of the
    track NAME (e.g. 'Brazilian_CR' -> 'Portuguese (Brazilian)'), dropping the
    fansub/source tags that make raw NAMEs ('CR', 'CR (2)') unreadable."""
    base = _LANG_NAMES.get((lang or "und").lower(), (lang or "Subtitle").title())
    region = re.sub(r"[\[\]()_]+", " ", name or "")          # separators -> space FIRST
    region = re.sub(r"\b(CR|WEB-?DL|WEB-?Rip|BD|Crunchyroll|Funi(?:mation)?|Netflix|NF|"
                    r"Amazon|AMZN|HIDIVE|Bilibili|Erai-?raws|SubsPlease|sub|dub|full)\b",
                    "", region, flags=re.I)                  # then strip source tags
    region = re.sub(r"\s{2,}", " ", region).strip(" -·").strip()
    # drop a bare dedup-counter number ('CR (3)' -> '3') or a region that just
    # restates the language ('English (English US)').
    if region.isdigit() or (region and base.lower() in region.lower()):
        region = ""
    return f"{base} ({region})" if region else base


async def _selfhost_source(http: httpx.AsyncClient, anilist_id: int, ep: int, category: str) -> Optional[dict]:
    """If the episode is cached on our self-hosted video origin, return it as a
    source (HLS master + subtitle VTTs). The whole HLS is proxied by watch.py just
    like any other source, so the origin IP stays hidden from the browser."""
    if not settings.SELFHOST_CACHE or not settings.SELFHOST_ORIGIN:
        return None
    # One canonical multi-audio build per episode (under "sub"); a dub is just
    # another audio track inside it, so serve the SAME build for either toggle.
    base = f"{settings.SELFHOST_ORIGIN}/{anilist_id}/{ep}/sub"
    try:
        r = await http.get(f"{base}/master.m3u8", timeout=5.0)
    except Exception:  # noqa: BLE001
        return None
    if r.status_code != 200 or "#EXTM3U" not in r.text[:64]:
        return None
    # audio tracks carried by the build (JP + any dubs), parsed from EXT-X-MEDIA AUDIO
    audios: list[dict] = []
    for line in r.text.splitlines():
        if line.startswith("#EXT-X-MEDIA:") and "TYPE=AUDIO" in line:
            nm = re.search(r'NAME="([^"]*)"', line)
            lg = re.search(r'LANGUAGE="([^"]*)"', line)
            audios.append({"name": nm.group(1) if nm else "Audio",
                           "lang": lg.group(1) if lg else "", "default": "DEFAULT=YES" in line})
    has_dub = any(not (a["lang"] or "").startswith("ja") for a in audios)
    # DUB toggle: only surface self-host if the build actually carries a non-JP audio
    # (else this episode is sub-only — fall back to other dub sources / sub).
    if category == "dub" and not has_dub:
        return None
    subs: list[dict] = []
    fonts: list[str] = []
    seen: dict[str, int] = {}

    def _add(lang, name, vtt_url, ass_url, default):
        label = _sub_label(lang, name)
        n = seen.get(label, 0) + 1; seen[label] = n
        subs.append({"label": label if n == 1 else f"{label} ({n})", "language": lang,
                     "file": vtt_url, "ass": ass_url, "default": default})

    # Prefer the subs/tracks.json manifest -> styled ASS + embedded fonts (faithful
    # JASSUB render). Fall back to the master's EXT-X-MEDIA SUBTITLES (VTT only).
    try:
        tr = await http.get(f"{base}/subs/tracks.json", timeout=4.0)
        if tr.status_code == 200:
            man = tr.json()
            for t in man.get("subs", []):
                _add(t.get("lang", ""), t.get("name", ""),
                     f"{base}/{t['vtt']}" if t.get("vtt") else None,
                     f"{base}/{t['ass']}" if t.get("ass") else None,
                     bool(t.get("default")))
            fonts = [f"{base}/{x}" for x in man.get("fonts", [])]
    except Exception:  # noqa: BLE001
        pass
    if not subs:
        for line in r.text.splitlines():
            if line.startswith("#EXT-X-MEDIA:") and "TYPE=SUBTITLES" in line:
                uri = re.search(r'URI="([^"]+)"', line)
                if not uri:
                    continue
                name = re.search(r'NAME="([^"]*)"', line)
                lang = re.search(r'LANGUAGE="([^"]*)"', line)
                _add(lang.group(1) if lang else "", name.group(1) if name else "",
                     f"{base}/{uri.group(1).rsplit('.', 1)[0]}.vtt", None, "DEFAULT=YES" in line)
    # for a DUB request, hint the player to default to a non-Japanese audio
    default_audio = None
    if category == "dub":
        nonjp = next((a for a in audios if not (a["lang"] or "").startswith("ja")), None)
        default_audio = nonjp["lang"] if nonjp else None
    return {"host": "anichan", "type": "hls", "label": "AniChan · self-hosted (ad-free)",
            "url": f"{base}/master.m3u8", "referer": "", "subtitles": subs,
            "fonts": fonts, "audios": audios, "default_audio": default_audio, "intro": None}


_ingest_fired: dict[tuple, float] = {}
_INGEST_TTL = 1800  # don't re-trigger the same (anime, ep) within 30 min


async def trigger_ingest(http: httpx.AsyncClient, anilist_id: int, ep: int) -> None:
    """Fire-and-forget: ask the video node to cache this episode (+ prefetch) when
    a user opens the page. Deduped per (anime, ep) so reloads don't spam it; the
    video node itself dedups vs cached/in-flight + caps concurrency and storage."""
    if not settings.SELFHOST_CACHE or not settings.SELFHOST_INGEST_URL:
        return
    key = (anilist_id, ep)
    now = time.time()
    if now - _ingest_fired.get(key, 0) < _INGEST_TTL:
        return
    _ingest_fired[key] = now
    if len(_ingest_fired) > 5000:
        _ingest_fired.clear()
    headers = {"X-Ingest-Token": settings.SELFHOST_INGEST_TOKEN} if settings.SELFHOST_INGEST_TOKEN else {}
    try:
        await http.get(f"{settings.SELFHOST_INGEST_URL}/ingest",
                       params={"anilist_id": anilist_id, "ep": ep}, headers=headers, timeout=4.0)
    except Exception:  # noqa: BLE001
        pass


_selfhost_cache: dict[tuple, tuple] = {}   # ckey -> (expiry, source_or_None)


async def _selfhost_cached(http: httpx.AsyncClient, anilist_id: int, ep: int, category: str) -> Optional[dict]:
    """_selfhost_source with a short, SEPARATE cache (positive 60s / negative 15s)
    so a freshly-cached episode appears as Source 1 within seconds — never baked
    into the 3-min Miruro list cache (which would hide it)."""
    ck = (anilist_id, ep, category)
    hit = _selfhost_cache.get(ck)
    if hit and time.time() < hit[0]:
        return hit[1]
    src = await _selfhost_source(http, anilist_id, ep, category)
    if len(_selfhost_cache) > 3000:
        _selfhost_cache.clear()
    _selfhost_cache[ck] = (time.time() + (60 if src else 15), src)
    return src


async def _miruro_servers(http: httpx.AsyncClient, anilist_id: int, ep: int, category: str) -> list[dict]:
    """The Miruro-resolved curated sources (cached `_SERVERS_TTL`)."""
    ckey = (anilist_id, ep, category)
    hit = _servers_cache.get(ckey)
    if hit and time.time() - hit[0] < _SERVERS_TTL:
        return hit[1]

    data = await _episodes_raw(http, anilist_id)
    if not data:
        return []
    idx = _index_providers(data)

    targets: list[tuple[str, str, str, str]] = []  # (host, mode, provider, ep_id), in display order
    for host, mode in RELIABLE_SOURCES:
        for prov, info in idx.items():
            if info["host"] != host:
                continue
            m = info["cats"].get(category) or {}
            if ep in m:
                targets.append((host, mode, prov, m[ep]))
                break

    async def _one(host: str, mode: str, prov: str, epid: str):
        src = await _pipe(
            http,
            {"path": "sources", "method": "GET",
             "query": {"episodeId": epid, "provider": prov, "category": category, "anilistId": anilist_id},
             "body": None, "version": "0.1.0"},
        )
        if not isinstance(src, dict):
            return None
        streams = src.get("streams") or []
        hls_mp4 = next((s for s in streams if s.get("type") in ("hls", "mp4") and s.get("url")), None)
        embeds = sorted((s for s in streams if s.get("type") == "embed" and s.get("url") and not _embed_blocked(s)), key=_epref)
        embed = embeds[0] if embeds else None
        # Pick per the host's mode, but FALL BACK to the other type so a host that
        # has the episode is never dropped just for lacking the preferred type.
        pick = (embed or hls_mp4) if mode == "embed" else (hls_mp4 or embed)
        if not pick:
            return None
        if pick.get("type") == "embed":
            return {"host": host, "type": "embed", "label": pick.get("server") or host,
                    "url": pick["url"], "referer": pick.get("referer") or "", "subtitles": []}
        return {"host": host, "type": pick["type"], "label": pick.get("server") or host,
                "url": pick["url"], "referer": pick.get("referer") or "",
                "subtitles": src.get("subtitles") or [], "intro": src.get("intro")}

    results = await asyncio.gather(*[_one(*t) for t in targets], return_exceptions=True)
    servers = [r for r in results if isinstance(r, dict)]  # already in display order
    if len(_servers_cache) > 3000:
        _servers_cache.clear()
    _servers_cache[ckey] = (time.time(), servers)
    return servers


async def resolve_all(http: httpx.AsyncClient, anilist_id: int, ep: int, category: str) -> list[dict]:
    """Curated sources for one episode — the self-hosted cache ranks #1 when
    present, then Miruro (clean first, embeds last). The two are resolved
    CONCURRENTLY and cached independently, so the self-host probe adds no latency
    and a freshly-cached episode surfaces promptly (not after the Miruro TTL)."""
    miruro, selfhost = await asyncio.gather(
        _miruro_servers(http, anilist_id, ep, category),
        _selfhost_cached(http, anilist_id, ep, category),
    )
    return [selfhost, *miruro] if selfhost else miruro
