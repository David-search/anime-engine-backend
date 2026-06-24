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
import ipaddress
import logging
import re
import urllib.parse

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response, StreamingResponse

from .. import sources
from ..config import settings
from ..db import get_db

router = APIRouter(prefix="/watch", tags=["watch"])
logger = logging.getLogger("anichan.watch")


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


def _safe(url: str) -> None:
    """Minimal SSRF guard: https/http only, no localhost / private ranges."""
    p = urllib.parse.urlsplit(url)
    if p.scheme not in ("http", "https") or not p.hostname:
        raise HTTPException(400, "bad url")
    host = p.hostname.lower()
    if host in ("localhost",) or host.endswith(".local"):
        raise HTTPException(400, "blocked host")
    try:
        ip = ipaddress.ip_address(host)
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
            raise HTTPException(400, "blocked host")
    except ValueError:
        pass  # hostname, not a literal IP — fine


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
    items = await sources.resolve_all(request.app.state.http, anilistId, ep, category)
    out = []
    for i, s in enumerate(items, 1):
        ref = s.get("referer", "")
        entry = {"name": f"source{i}", "label": s["label"], "host": s["host"], "type": s["type"]}
        if s["type"] == "embed":
            entry["embed"] = s["url"]
        else:
            entry["stream"] = _proxy("m3u8" if s["type"] == "hls" else "seg", s["url"], ref)
            entry["subtitles"] = [
                {"lang": x.get("label") or x.get("language") or "Subtitle",
                 "url": _proxy("vtt", x["file"], ref), "default": bool(x.get("default"))}
                for x in (s.get("subtitles") or []) if x.get("file")
            ]
            entry["intro"] = s.get("intro")
        out.append(entry)
    return {"servers": out}


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
            m = _KEY_URI.search(s)  # EXT-X-KEY / EXT-X-MAP carry a URI to proxy
            if m:
                s = s.replace(m.group(1), _proxy("seg", _abs(base, m.group(1)), ref))
            out.append(s)
            continue
        kind = "m3u8" if ".m3u8" in s.lower() else "seg"
        out.append(_proxy(kind, _abs(base, s), ref))

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
