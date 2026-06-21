from .allanime import AllAnimeProvider
from .base import AnimeProvider
from .megaplay import MegaPlayProvider

_PROVIDERS: dict[str, AnimeProvider] = {
    "megaplay": MegaPlayProvider(),  # primary: AniList-keyed, no Cloudflare, sub+dub, multi-lang
    "allanime": AllAnimeProvider(),  # fallback (needs clean IP / VPN for Cloudflare)
}

DEFAULT = "megaplay"


def get_provider(name: str | None = None) -> AnimeProvider:
    return _PROVIDERS.get(name or DEFAULT, _PROVIDERS[DEFAULT])


def provider_names() -> list[str]:
    return list(_PROVIDERS)
