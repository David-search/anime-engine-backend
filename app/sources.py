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
import time
from typing import Any, Optional

import httpx

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
    maxep = 0
    for info in idx.values():
        by_host.setdefault(info["host"], info)
        for m in info["cats"].values():
            nums = [n for n in m if isinstance(n, int)]
            if nums:
                maxep = max(maxep, max(nums))
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


async def resolve_all(http: httpx.AsyncClient, anilist_id: int, ep: int, category: str) -> list[dict]:
    """Resolve the curated sources for one episode: one entry per RELIABLE_SOURCES
    host, using its clean stream (hls/mp4) or its embed, per the host's mode.
    Returned in display order — clean first, embeds last. Cached briefly.
    """
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
