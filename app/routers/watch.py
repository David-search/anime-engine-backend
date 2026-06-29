"""
Watch endpoints: resolve playable streams via the Miruro pipe (app.sources) and
proxy the HLS playlist / segments / subtitles with the per-stream Referer the
host CDN requires (browsers can't set Referer, and the CDNs send no CORS).

    GET /api/watch/episodes?anilistId=         -> ranked source list + ep count
    GET /api/watch/sources?anilistId&source&ep&category -> proxied stream + subs
    GET /api/watch/m3u8?url=&ref=              -> rewritten master/variant playlist
    GET /api/watch/seg?url=&ref=               -> segment/key bytes (Range-aware)
    GET /api/watch/vtt?url=&ref=               -> subtitle track (text/vtt)
"""
import asyncio
import ipaddress
import logging
import re
import socket
import time
import urllib.parse

import httpx
from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import Response, StreamingResponse

from .. import sources
from ..config import settings
from ..db import get_db

router = APIRouter(prefix="/watch", tags=["watch"])
logger = logging.getLogger("anichan.watch")

_bg_tasks: set = set()


def _bg(coro) -> None:
    """Fire-and-forget a coroutine, keeping a strong ref so the event loop can't
    garbage-collect the task before it completes."""
    t = asyncio.ensure_future(coro)
    _bg_tasks.add(t)
    t.add_done_callback(_bg_tasks.discard)


async def _title(anilist_id: int) -> str:
    try:
        db = get_db()
        doc = await db.anime.find_one({"_id": anilist_id}, {"title": 1}) if db is not None else None
        return (doc or {}).get("title") or f"#{anilist_id}"
    except Exception:  # noqa: BLE001
        return f"#{anilist_id}"

_KEY_URI = re.compile(r'URI="([^"]+)"')


def _proxy(kind: str, url: str, ref: str) -> str:
    """Root-relative proxy URL (resolves against the playlist's backend origin)."""
    return f"/api/watch/{kind}?" + urllib.parse.urlencode({"url": url, "ref": ref})


def _emit(kind: str, url: str, ref: str) -> str:
    """For a self-hosted-origin URL, emit a DIRECT CDN URL (offloads the bytes off
    this proxy onto the edge) when SELFHOST_CDN_BASE is set; else fall back to
    _proxy(). Only the heavy nested audio/video/segment/subtitle/font URLs go
    direct — the master playlist itself still goes through _proxy() so its
    in-manifest subtitle groups get stripped. Non-self-host (Miruro) URLs always
    fall back to the proxy. Swapping/retiring the CDN is a DNS flip, no redeploy."""
    cdn = settings.SELFHOST_CDN_BASE
    origin = settings.SELFHOST_ORIGIN
    if cdn and origin and url.startswith(origin):
        return cdn + url[len(origin):]
    return _proxy(kind, url, ref)


_safe_cache: dict[str, float] = {}   # host -> expiry of a passed check
_SAFE_TTL = 300


def _safe(url: str) -> None:
    """SSRF guard: http(s) only; RESOLVE the host and reject if any resolved
    address is private/loopback/link-local/reserved/unspecified/multicast. This
    blocks octal/decimal/hex IP-literal encodings and hostnames that point at
    internal or cloud-metadata IPs (169.254.169.254). Results cached per host."""
    p = urllib.parse.urlsplit(url)
    if p.scheme not in ("http", "https") or not p.hostname:
        raise HTTPException(400, "bad url")
    host = p.hostname.lower()
    if _safe_cache.get(host, 0.0) > time.time():
        return
    if host == "localhost" or host.endswith((".local", ".internal")):
        raise HTTPException(400, "blocked host")
    try:
        infos = socket.getaddrinfo(host, p.port or (443 if p.scheme == "https" else 80),
                                   proto=socket.IPPROTO_TCP)
    except socket.gaierror:
        raise HTTPException(400, "unresolvable host")
    for info in infos:
        try:
            ip = ipaddress.ip_address(info[4][0].split("%", 1)[0])
        except ValueError:
            raise HTTPException(400, "blocked host")
        if (ip.is_private or ip.is_loopback or ip.is_link_local
                or ip.is_reserved or ip.is_unspecified or ip.is_multicast):
            raise HTTPException(400, "blocked host")
    if len(_safe_cache) > 4096:
        _safe_cache.clear()
    _safe_cache[host] = time.time() + _SAFE_TTL


def _abs(base: str, u: str) -> str:
    return u if u.startswith(("http://", "https://")) else urllib.parse.urljoin(base, u)


def _fwd(ref: str) -> dict:
    """Headers a host CDN expects: UA + Referer + a matching Origin (many CDNs
    400 the segment without Origin even when the playlist fetched fine)."""
    h = {"User-Agent": settings.USER_AGENT}
    if ref:
        h["Referer"] = ref
        p = urllib.parse.urlsplit(ref)
        if p.scheme and p.netloc:
            h["Origin"] = f"{p.scheme}://{p.netloc}"
    return h


# ── resolution ────────────────────────────────────────────────────────────────

@router.get("/episodes")
async def episodes(request: Request, anilistId: int):
    return await sources.list_sources(request.app.state.http, anilistId)


@router.get("/servers")
async def servers(request: Request, anilistId: int, ep: int = 1, category: str = "sub"):
    """Curated source list for one episode (reliable hosts, clean first then
    embeds). hls/mp4 play via our proxy; embed is a sandboxed iframe URL."""
    logger.info("▶ watch · %s · ep %s · %s", await _title(anilistId), ep, category)
    # DISABLED (2026-06-26): no more auto-triggered downloads. Self-host builds are
    # now a manual / build-farm step, not kicked off by a viewer opening a page.
    # _bg(sources.trigger_ingest(request.app.state.http, anilistId, ep))
    items = await sources.resolve_all(request.app.state.http, anilistId, ep, category)
    out = []
    for i, s in enumerate(items, 1):
        ref = s.get("referer", "")
        entry = {"name": f"source{i}", "label": s["label"], "host": s["host"], "type": s["type"]}
        if s["type"] == "embed":
            entry["embed"] = s["url"]
        else:
            entry["stream"] = _proxy("m3u8" if s["type"] == "hls" else "seg", s["url"], ref)
            subs_out = []
            for x in (s.get("subtitles") or []):
                if not x.get("file") and not x.get("ass"):
                    continue
                so = {"lang": x.get("label") or x.get("language") or "Subtitle",
                      "default": bool(x.get("default"))}
                if x.get("file"):
                    so["url"] = _emit("vtt", x["file"], ref)         # WebVTT (fallback)
                if x.get("ass"):
                    so["ass"] = _emit("seg", x["ass"], ref)          # styled ASS (JASSUB)
                subs_out.append(so)
            entry["subtitles"] = subs_out
            if s.get("fonts"):  # embedded fonts for faithful ASS rendering
                entry["fonts"] = [_emit("seg", f, ref) for f in s["fonts"]]
            if s.get("audios"):  # multi-audio (JP + dubs); player builds an audio selector
                entry["audios"] = s["audios"]
            if s.get("default_audio"):  # DUB toggle -> default to a non-JP track
                entry["defaultAudio"] = s["default_audio"]
            entry["intro"] = s.get("intro")
        out.append(entry)
    return {"servers": out}


@router.post("/cache-state")
async def cache_state(request: Request, x_ingest_token: str = Header(default="")):
    """Video node → backend: which episodes of an anime are now cached + their
    ani.zip episode titles. Token-auth'd (shared SELFHOST_INGEST_TOKEN). Upserts the
    `selfhost_cache` collection the catalog reads for coverage badges + episode
    titles — the node's cache_db stays the source of truth; this is a read-index."""
    if not settings.SELFHOST_INGEST_TOKEN or x_ingest_token != settings.SELFHOST_INGEST_TOKEN:
        raise HTTPException(401, "unauthorized")
    body = await request.json()
    try:
        aid = int(body["anilist_id"])
    except (KeyError, TypeError, ValueError):
        raise HTTPException(400, "bad anilist_id")
    db = get_db()
    if db is None:
        raise HTTPException(503, "no db")
    cached = body.get("cached") or {}
    # MERGE with existing coverage so a resumed/partial run never regresses prior episodes,
    # and fill total_eps from the catalog so callers can compute which episodes are still MISSING
    # (uncached_sub = {1..total_eps} - cached.sub) — the basis for future repopulation passes.
    prev = await db.selfhost_cache.find_one({"_id": aid}) or {}
    pc = prev.get("cached") or {}
    merged = {cat: sorted(set(pc.get(cat) or []) | set(cached.get(cat) or [])) for cat in ("sub", "dub")}
    anime = await db.anime.find_one({"_id": aid}, {"episodes": 1})
    total = (anime or {}).get("episodes") or body.get("total_eps") or prev.get("total_eps")
    ep_titles = {**(prev.get("ep_titles") or {}), **(body.get("ep_titles") or {})}
    await db.selfhost_cache.update_one(
        {"_id": aid},
        {"$set": {"cached": merged, "ep_titles": ep_titles, "total_eps": total,
                  "updated_at": int(time.time())}},
        upsert=True)
    return {"ok": True, "anilist_id": aid, "sub": len(merged["sub"]),
            "dub": len(merged["dub"]), "total_eps": total}


@router.get("/sources")
async def get_sources(request: Request, anilistId: int, source: str, ep: int = 1, category: str = "sub"):
    res = await sources.resolve(request.app.state.http, anilistId, source, ep, category)
    if not res:
        raise HTTPException(404, "source unavailable")
    if res["type"] == "embed":
        return {"type": "embed", "embed": res["url"]}
    ref = res.get("referer", "")
    subs = [
        {"lang": s.get("label") or s.get("language") or "Subtitle",
         "url": _proxy("vtt", s["file"], ref),
         "default": bool(s.get("default"))}
        for s in res.get("subtitles", []) if s.get("file")
    ]
    return {
        "type": "hls",
        "stream": _proxy("m3u8", res["url"], ref),
        "subtitles": subs,
        "intro": res.get("intro"),
        "outro": res.get("outro"),
    }


# ── proxy ────────────────────────────────────────────────────────────────────

@router.get("/m3u8")
async def m3u8(request: Request, url: str, ref: str = ""):
    _safe(url)
    http: httpx.AsyncClient = request.app.state.http
    try:
        r = await http.get(url, headers=_fwd(ref), timeout=20.0)
    except Exception:  # noqa: BLE001
        raise HTTPException(502, "upstream fetch failed")
    if r.status_code != 200:
        raise HTTPException(502, f"upstream {r.status_code}")

    base = url.rsplit("/", 1)[0] + "/"
    out: list[str] = []
    for line in r.text.splitlines():
        s = line.strip()
        if not s:
            out.append(line)
            continue
        if s.startswith("#"):
            # Subtitles are delivered via the `subtitles` array (player <track>s), not
            # in-manifest — drop the EXT-X-MEDIA subtitle group + its STREAM-INF ref so
            # hls.js doesn't add extra textTracks that desync the subtitle selector.
            if s.startswith("#EXT-X-MEDIA") and "TYPE=SUBTITLES" in s:
                continue
            if s.startswith("#EXT-X-STREAM-INF"):
                # drop the removed subtitle group ref whether it's the first attr
                # (':SUBTITLES=…,') or a later one (',SUBTITLES=…') — no dangling comma
                s = re.sub(r'(:)SUBTITLES="[^"]*",?', r"\1", s)
                s = re.sub(r',SUBTITLES="[^"]*"', "", s)
            m = _KEY_URI.search(s)
            if m:
                # a URI targeting a PLAYLIST (.m3u8 — audio group / I-FRAME variant)
                # is proxied as m3u8; KEY/MAP byte URIs as seg.
                uri = m.group(1)
                kind = "m3u8" if (".m3u8" in uri.lower() or s.startswith("#EXT-X-MEDIA")) else "seg"
                s = s.replace(uri, _emit(kind, _abs(base, uri), ref))
            out.append(s)
            continue
        kind = "m3u8" if ".m3u8" in s.lower() else "seg"
        out.append(_emit(kind, _abs(base, s), ref))

    return Response("\n".join(out), media_type="application/vnd.apple.mpegurl",
                    headers={"Cache-Control": "no-store"})


@router.get("/seg")
async def seg(request: Request, url: str, ref: str = ""):
    _safe(url)
    http: httpx.AsyncClient = request.app.state.http
    fwd = _fwd(ref)
    if rng := request.headers.get("range"):
        fwd["Range"] = rng
    try:
        upstream = await http.send(http.build_request("GET", url, headers=fwd), stream=True)
    except Exception:  # noqa: BLE001
        raise HTTPException(502, "segment fetch failed")

    headers = {"Cache-Control": "public, max-age=3600"}
    for h in ("content-length", "content-range", "accept-ranges"):
        if h in upstream.headers:
            headers[h] = upstream.headers[h]

    async def body():
        try:
            async for chunk in upstream.aiter_bytes():
                yield chunk
        finally:
            await upstream.aclose()

    return StreamingResponse(body(), status_code=upstream.status_code, headers=headers,
                             media_type=upstream.headers.get("content-type", "application/octet-stream"))


@router.get("/vtt")
async def vtt(request: Request, url: str, ref: str = ""):
    """Subtitle proxy. These URLs are session-bound/flaky and referer-sensitive,
    so try the stream referer -> miruro -> none, and only serve real WEBVTT;
    otherwise 404 so a broken track never blocks playback."""
    _safe(url)
    http: httpx.AsyncClient = request.app.state.http
    seen: set[str] = set()
    for r in (ref, "https://www.miruro.to/", ""):
        if r in seen:
            continue
        seen.add(r)
        headers = {"User-Agent": settings.USER_AGENT}
        if r:
            headers["Referer"] = r
        try:
            resp = await http.get(url, headers=headers, timeout=15.0)
        except Exception:  # noqa: BLE001
            continue
        if resp.status_code == 200 and b"WEBVTT" in resp.content[:64]:
            return Response(resp.content, media_type="text/vtt",
                            headers={"Cache-Control": "public, max-age=86400"})
    raise HTTPException(404, "subtitle unavailable")
