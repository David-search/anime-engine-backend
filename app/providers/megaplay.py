"""MegaPlay / VidWish host integration (the primary, working source).

Discovery (AniList -> realid):  megaplay.buzz/stream/ani/{anilistId}/{ep}/{cat}
                                -> embed HTML with data-realid (HiAnime episode id)
Stream    (realid -> m3u8):     vidwish.live/stream/s-2/{realid}/{cat}
                                -> data-id -> /stream/getSources?id=... (plaintext)
                                -> { sources.file (m3u8 on *.watching.onl), tracks(vtt), intro, outro }

Why VidWish for the stream: MegaPlay's own CDN (cdn.mewstream.buzz) 403s every header
combo, but VidWish's CDN (*.watching.onl) serves the SAME content with
`Referer: https://vidwish.live/`. They share the realid (same operator/library).

No Cloudflare, no browser — curl_cffi (Chrome TLS impersonation) is enough.
See claude/research/host-integration-findings.md.
"""
from __future__ import annotations

import re

from curl_cffi.requests import AsyncSession

from .base import (
    AnimeInfo,
    AnimeProvider,
    Category,
    Episode,
    ProviderError,
    SearchResult,
    Server,
    Skip,
    Sources,
    Subtitle,
)

MEGA = "https://megaplay.buzz"
WISH = "https://vidwish.live"
AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def _skip(x) -> Skip | None:
    if isinstance(x, dict) and x.get("end"):
        return Skip(start=float(x.get("start", 0)), end=float(x["end"]))
    return None


class MegaPlayProvider(AnimeProvider):
    id = "megaplay"
    anilist_native = True  # episode_id == "{anilistId}/{ep}", no mapping step

    async def _realid(self, sess: AsyncSession, anilist_id: str, ep: str, cat: str) -> str:
        url = f"{MEGA}/stream/ani/{anilist_id}/{ep}/{cat}"
        r = await sess.get(url, headers={"Referer": MEGA + "/", "User-Agent": AGENT}, impersonate="chrome", timeout=20)
        m = re.search(r'data-realid="(\d+)"', r.text)
        if not m:
            raise ProviderError(f"megaplay: no realid for anilist={anilist_id} ep={ep} {cat}")
        return m.group(1)

    async def _vidwish(self, sess: AsyncSession, realid: str, cat: str):
        emb = f"{WISH}/stream/s-2/{realid}/{cat}"
        r = await sess.get(emb, headers={"Referer": WISH + "/", "User-Agent": AGENT}, impersonate="chrome", timeout=20)
        m = re.search(r'data-id="(\d+)"', r.text)
        if not m:
            raise ProviderError(f"vidwish: no data-id for realid={realid} {cat}")
        gs = await sess.get(
            f"{WISH}/stream/getSources?id=" + m.group(1),
            headers={"Referer": emb, "X-Requested-With": "XMLHttpRequest", "User-Agent": AGENT},
            impersonate="chrome",
            timeout=20,
        )
        d = gs.json()
        src = d.get("sources")
        file = src["file"] if isinstance(src, dict) else (src[0]["file"] if src else None)
        if not file:
            raise ProviderError("vidwish: getSources had no file")
        subs = [
            Subtitle(url=t["file"], lang=t.get("label", "Unknown"), default=bool(t.get("default")))
            for t in (d.get("tracks") or [])
            if t.get("kind") == "captions" and t.get("file")
        ]
        return file, subs, _skip(d.get("intro")), _skip(d.get("outro"))

    async def sources(self, client, episode_id: str, server: str, category: Category) -> Sources:
        anilist_id, _, ep = episode_id.partition("/")
        cat = "dub" if category == "dub" else "sub"
        async with AsyncSession() as sess:
            realid = await self._realid(sess, anilist_id, ep, cat)
            file, subs, intro, outro = await self._vidwish(sess, realid, cat)
        return Sources(
            url=file,
            is_m3u8=".m3u8" in file,
            headers={"Referer": WISH + "/"},
            subtitles=subs,
            intro=intro,
            outro=outro,
        )

    # --- interface stubs: catalog is AniList, so these aren't the primary path ---
    async def search(self, client, query: str) -> list[SearchResult]:
        return []

    async def info(self, client, anime_id: str) -> AnimeInfo:
        return AnimeInfo(id=anime_id, title="")

    async def episodes(self, client, anime_id: str) -> list[Episode]:
        return []

    async def servers(self, client, episode_id: str, category: Category) -> list[Server]:
        return [Server(name="vidwish", category=category)]
