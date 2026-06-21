"""AllAnime provider — the first real stream source.

Flow (mirrors ani-cli, proven against the live API):
  search  -> shows(search) -> show _id
  episodes-> show(availableEpisodesDetail) -> episode numbers per sub/dub/raw
  sources -> episode(sourceUrls) -> internal "--<hex>" urls
          -> decode (hex pairs XOR 56) -> /apivtwo/clock?id=.. path
          -> fetch clock.json -> { links: [{ link (.m3u8/.mp4), hls, ... }] }

api.allanime.day is behind Cloudflare Turnstile, so every call goes through
curl_cffi (Chrome TLS impersonation) carrying the cf_clearance cookie harvested
by app.cf. If clearance is missing/expired we raise ProviderBlocked with a hint.
"""
from __future__ import annotations

import json

import httpx

from .. import cf
from .base import (
    AnimeInfo,
    AnimeProvider,
    Category,
    Episode,
    ProviderBlocked,
    ProviderError,
    SearchResult,
    Server,
    Sources,
)

API = "https://api.allanime.day/api"
SITE = "https://allanime.day"
REFERER = "https://allmanga.to"

SEARCH_Q = (
    "query($search:SearchInput,$limit:Int,$page:Int,$translationType:VaildTranslationTypeEnumType,"
    "$countryOrigin:VaildCountryOriginEnumType){shows(search:$search,limit:$limit,page:$page,"
    "translationType:$translationType,countryOrigin:$countryOrigin){edges{_id name availableEpisodes thumbnail}}}"
)
INFO_Q = "query($showId:String!){show(_id:$showId){_id name thumbnail description availableEpisodes}}"
EPISODES_Q = "query($showId:String!){show(_id:$showId){_id availableEpisodesDetail}}"
SOURCES_Q = (
    "query($showId:String!,$translationType:VaildTranslationTypeEnumType!,$episodeString:String!){"
    "episode(showId:$showId,translationType:$translationType,episodeString:$episodeString){episodeString sourceUrls}}"
)

REFRESH_HINT = "Cloudflare clearance missing/expired. Run: ./.venv/bin/python -m app.cf_refresh"


def _decode_source_url(s: str) -> str | None:
    """allanime internal source: '--' + hex, each byte XOR 56 -> a /apivtwo/clock path."""
    if not s or not s.startswith("--"):
        return None
    h = s[2:]
    try:
        return "".join(chr(int(h[i : i + 2], 16) ^ 56) for i in range(0, len(h), 2))
    except ValueError:
        return None


def _num(x: str) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


class AllAnimeProvider(AnimeProvider):
    id = "allanime"

    async def _get(self, url: str) -> dict:
        # Routed through the persistent browser session, which holds the
        # Cloudflare clearance and matches its own TLS fingerprint.
        return await cf.manager.fetch_json(url)

    async def _gql(self, query: str, variables: dict) -> dict:
        url = str(httpx.URL(API, params={"variables": json.dumps(variables), "query": query}))
        data = await self._get(url)
        if data.get("errors"):
            raise ProviderError(str(data["errors"]))
        return data["data"]

    async def search(self, client, query: str) -> list[SearchResult]:
        variables = {
            "search": {"allowAdult": False, "allowUnknown": False, "query": query},
            "limit": 40,
            "page": 1,
            "translationType": "sub",
            "countryOrigin": "ALL",
        }
        data = await self._gql(SEARCH_Q, variables)
        out: list[SearchResult] = []
        for e in data["shows"]["edges"]:
            ae = e.get("availableEpisodes") or {}
            out.append(
                SearchResult(
                    id=e["_id"],
                    title=e.get("name") or "",
                    poster=e.get("thumbnail"),
                    sub=ae.get("sub"),
                    dub=ae.get("dub"),
                )
            )
        return out

    async def info(self, client, anime_id: str) -> AnimeInfo:
        data = await self._gql(INFO_Q, {"showId": anime_id})
        s = data["show"]
        ae = s.get("availableEpisodes") or {}
        return AnimeInfo(
            id=anime_id,
            title=s.get("name") or "",
            description=s.get("description"),
            poster=s.get("thumbnail"),
            total_episodes=ae.get("sub"),
        )

    async def episodes(self, client, anime_id: str, category: Category = "sub") -> list[Episode]:
        data = await self._gql(EPISODES_Q, {"showId": anime_id})
        detail = data["show"].get("availableEpisodesDetail") or {}
        nums = detail.get(category) or detail.get("sub") or []
        eps = sorted(set(nums), key=_num)
        return [Episode(id=f"{anime_id}/{e}", number=_num(e)) for e in eps]

    async def servers(self, client, episode_id: str, category: Category) -> list[Server]:
        # allanime has no discrete servers; the sourceUrls are the variants.
        return [Server(name="default", category=category)]

    async def sources(self, client, episode_id: str, server: str, category: Category) -> Sources:
        show_id, _, ep = episode_id.rpartition("/")
        data = await self._gql(
            SOURCES_Q,
            {"showId": show_id, "translationType": category, "episodeString": ep},
        )
        episode = data.get("episode") or {}
        links: list[dict] = []
        for src in episode.get("sourceUrls") or []:
            decoded = _decode_source_url(src.get("sourceUrl", ""))
            if not decoded or not decoded.startswith("/"):
                continue
            path = decoded.replace("/clock?", "/clock.json?")
            try:
                clock = await self._get(SITE + path)
            except ProviderError:
                continue
            for l in clock.get("links") or []:
                link = l.get("link")
                if link:
                    links.append({"link": link, "hls": bool(l.get("hls")), "res": l.get("resolutionStr")})

        if not links:
            raise ProviderError("no playable links resolved")

        # Prefer an HLS (.m3u8) link; fall back to the first (often mp4).
        best = next((l for l in links if ".m3u8" in l["link"]), None) or links[0]
        is_m3u8 = ".m3u8" in best["link"] or best["hls"]
        return Sources(
            url=best["link"],
            is_m3u8=is_m3u8,
            headers={"Referer": REFERER + "/"},
        )
