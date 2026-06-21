"""Stream resolution endpoint.

Resolves AniList id + episode -> a host's .m3u8 (+ subtitles), wrapped through our
proxy. The default provider (megaplay) is AniList-keyed, so no title mapping is
needed. On any failure it degrades to the test stub with the reason in `error`.

Force the stub with ?provider=stub.
"""
from fastapi import APIRouter, Request

from .. import mapping
from ..config import settings
from ..providers.base import ProviderBlocked
from ..providers.registry import get_provider
from ..proxy import build_proxy_url

router = APIRouter()


def _stub(request: Request, anime_id, ep, category, server, error=None):
    base = str(request.base_url).rstrip("/")
    return {
        "provider": "stub" if not error else "stub (fallback)",
        "animeId": anime_id, "episode": ep, "category": category, "server": server,
        "headers": {}, "rawUrl": settings.TEST_STREAM,
        "url": build_proxy_url(base, settings.TEST_STREAM, {}),
        "isM3U8": True, "subtitles": [], "intro": None, "outro": None,
        "servers": [{"name": "default", "category": category}], "error": error,
    }


@router.get("/sources")
async def sources(
    request: Request,
    anime_id: str = "",
    ep: str = "1",
    category: str = "sub",
    server: str = "default",
    provider: str = "megaplay",
):
    if provider == "stub" or not anime_id:
        return _stub(request, anime_id, ep, category, server)

    client = request.app.state.http
    prov = get_provider(provider)
    base = str(request.base_url).rstrip("/")

    try:
        if getattr(prov, "anilist_native", False):
            episode_id = f"{anime_id}/{ep}"
        else:
            pid = await mapping.resolve(client, prov, int(anime_id))
            if not pid:
                return _stub(request, anime_id, ep, category, server, error="no matching title on provider")
            episode_id = f"{pid}/{ep}"

        s = await prov.sources(client, episode_id, server, category)
        subtitles = [
            {"url": build_proxy_url(base, sub.url, s.headers), "lang": sub.lang, "default": sub.default}
            for sub in s.subtitles
        ]
        return {
            "provider": prov.id,
            "animeId": anime_id, "episode": ep, "category": category, "server": server,
            "headers": s.headers,
            "rawUrl": s.url,
            "url": build_proxy_url(base, s.url, s.headers),
            "isM3U8": s.is_m3u8,
            "subtitles": subtitles,
            "intro": s.intro.model_dump() if s.intro else None,
            "outro": s.outro.model_dump() if s.outro else None,
            "servers": [{"name": "vidwish", "category": category}],
            "error": None,
        }
    except ProviderBlocked as e:
        return _stub(request, anime_id, ep, category, server, error=f"blocked: {e}")
    except Exception as e:  # noqa: BLE001
        return _stub(request, anime_id, ep, category, server, error=f"{type(e).__name__}: {e}")
