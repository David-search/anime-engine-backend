"""The m3u8 / segment / key CORS proxy.

Why this exists: upstream video hosts gate their .m3u8/.ts/.key behind a
`Referer`/`Origin` check and serve no CORS headers, so the browser cannot fetch
them directly. This proxy re-fetches them server-side with the required headers,
rewrites every URL inside a playlist to also route back through here, and serves
the result with permissive CORS so hls.js can play it.

`build_proxy_url()` is imported by the sources router to wrap a raw stream URL.
"""
from __future__ import annotations

import base64
import json
from urllib.parse import urljoin

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import PlainTextResponse, Response, StreamingResponse
from starlette.background import BackgroundTask

from .config import settings

router = APIRouter()


def _b64e(s: str) -> str:
    return base64.urlsafe_b64encode(s.encode()).decode()


def _b64d(s: str) -> str:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad).decode()


def build_proxy_url(base_url: str, target_url: str, headers: dict | None = None) -> str:
    """base_url e.g. 'http://localhost:8000' -> '<base>/api/proxy?u=..&h=..'."""
    u = _b64e(target_url)
    h = _b64e(json.dumps(headers or {}))
    return f"{base_url}/api/proxy?u={u}&h={h}"


def rewrite_playlist(text: str, playlist_url: str, base_url: str, headers: dict) -> str:
    """Rewrite every URI in an HLS playlist to route back through this proxy."""
    out: list[str] = []
    for line in text.splitlines():
        s = line.strip()
        if not s:
            out.append(line)
            continue
        if s.startswith("#"):
            # EXT-X-KEY / EXT-X-MAP / EXT-X-MEDIA / I-FRAME carry URI="..."
            if 'URI="' in s:
                pre, rest = s.split('URI="', 1)
                uri, post = rest.split('"', 1)
                proxied = build_proxy_url(base_url, urljoin(playlist_url, uri), headers)
                out.append(f'{pre}URI="{proxied}"{post}')
            else:
                out.append(line)
            continue
        # A bare line is a URI: a variant playlist or a media segment.
        out.append(build_proxy_url(base_url, urljoin(playlist_url, s), headers))
    return "\n".join(out)


def _is_playlist(url: str) -> bool:
    return ".m3u8" in url.lower().split("?", 1)[0]


@router.get("/proxy")
async def proxy(request: Request):
    u = request.query_params.get("u")
    if not u:
        raise HTTPException(400, "missing 'u'")
    target = _b64d(u)
    h = request.query_params.get("h")
    custom_headers: dict = json.loads(_b64d(h)) if h else {}

    client: httpx.AsyncClient = request.app.state.http
    base_url = str(request.base_url).rstrip("/")

    up_headers = {"User-Agent": settings.USER_AGENT}
    up_headers.update(custom_headers)
    rng = request.headers.get("range")
    if rng:
        up_headers["Range"] = rng

    if _is_playlist(target):
        r = await client.get(target, headers=up_headers)
        ct = r.headers.get("content-type", "")
        text = r.text
        if "mpegurl" in ct.lower() or text.lstrip().startswith("#EXTM3U"):
            rewritten = rewrite_playlist(text, str(r.url), base_url, custom_headers)
            return PlainTextResponse(rewritten, media_type="application/vnd.apple.mpegurl")
        # Not actually a playlist — fall through and return raw bytes.
        return Response(content=r.content, media_type=ct or "application/octet-stream")

    # Segment / key / subtitle: stream bytes through, preserving range semantics.
    req = client.build_request("GET", target, headers=up_headers)
    resp = await client.send(req, stream=True)
    passthru = {
        k: v
        for k, v in resp.headers.items()
        if k.lower() in ("content-type", "content-length", "accept-ranges", "content-range")
    }
    return StreamingResponse(
        resp.aiter_bytes(),
        status_code=resp.status_code,
        headers=passthru,
        background=BackgroundTask(resp.aclose),
    )
