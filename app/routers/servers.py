"""Server discovery — only hosts that actually play when embedded on OUR domain.

Hard-won lesson: Anikoto's internal hosts (VidWish, MegaPlay-HD/s-5, VidTube) are
DOMAIN-LOCKED — their player only serves the stream to whitelisted embedders
(anikoto.*). They look fine (getSources returns a master URL) but the variants
404 for outsiders, so they never play on our site. We exclude them.

What works cross-domain:
- MegaPlay /stream/ani — Anikoto's PUBLIC webmaster endpoint (embeds anywhere)
- VidLink / VidNest — independent public embedders (different operators), MAL-keyed
"""
import re

from curl_cffi.requests import AsyncSession
from fastapi import APIRouter, Request

from .. import anilist

router = APIRouter()

MEGA = "https://megaplay.buzz"
AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

_cache: dict = {}


async def _in_library(sess: AsyncSession, anilist_id: int, ep: int) -> bool:
    """MegaPlay's public endpoint exposes data-realid only when it has the title."""
    try:
        r = await sess.get(
            f"{MEGA}/stream/ani/{anilist_id}/{ep}/sub",
            headers={"Referer": MEGA + "/", "User-Agent": AGENT},
            impersonate="chrome", timeout=12,
        )
        return bool(re.search(r'data-realid="(\d+)"', r.text))
    except Exception:
        return False


@router.get("/servers")
async def servers(request: Request, anime_id: int, ep: int = 1):
    key = (anime_id, ep)
    if key in _cache:
        return _cache[key]

    media = await anilist.get_media(request.app.state.http, anime_id)
    mal = media.get("idMal") if media else None
    out: list[dict] = []

    async with AsyncSession() as sess:
        if await _in_library(sess, anime_id, ep):
            out.append({"id": "megaplay", "label": "MegaPlay",
                        "sub": f"{MEGA}/stream/ani/{anime_id}/{ep}/sub",
                        "dub": f"{MEGA}/stream/ani/{anime_id}/{ep}/dub"})

    # Independent public embedders (different operators) — embed anywhere, but we
    # can't cheaply confirm they hold a given title, so they're best-effort (beta).
    if mal:
        out.append({"id": "vidlink", "label": "VidLink", "beta": True,
                    "sub": f"https://vidlink.pro/anime/{mal}/{ep}/sub",
                    "dub": f"https://vidlink.pro/anime/{mal}/{ep}/dub"})
        out.append({"id": "vidnest", "label": "VidNest", "beta": True,
                    "sub": f"https://vidnest.fun/anime/{mal}/{ep}/sub",
                    "dub": f"https://vidnest.fun/anime/{mal}/{ep}/dub"})

    resp = {"anilist": anime_id, "ep": ep, "mal": mal, "servers": out}
    _cache[key] = resp
    return resp
