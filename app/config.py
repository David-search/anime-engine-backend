import os


class Settings:
    CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*")
    USER_AGENT = os.getenv(
        "USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    )

    # Data stores (optional — endpoints degrade to 503 / AniList fallback if unset)
    MONGO_URI = os.getenv("MONGO_URI", "")
    MONGO_DB = os.getenv("MONGO_DB", "anime_db")
    ELASTIC_URL = os.getenv("ELASTIC_URL", "")
    ELASTIC_USER = os.getenv("ELASTIC_USER", "elastic")
    ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD", "")
    ELASTIC_INDEX = os.getenv("ELASTIC_INDEX", "anime")

    # Auth
    JWT_SECRET = os.getenv("JWT_SECRET", "dev-insecure-change-me")
    JWT_TTL_DAYS = int(os.getenv("JWT_TTL_DAYS", "30"))
    GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")

    # Self-hosted video origin (the AniChan video tier). When SELFHOST_CACHE=1 and
    # an episode is cached at SELFHOST_ORIGIN/{anilistId}/{ep}/{cat}/master.m3u8, it
    # is offered as Source 1 (ad-free), proxied like any other HLS source.
    SELFHOST_CACHE = os.getenv("SELFHOST_CACHE", "0") == "1"
    SELFHOST_ORIGIN = os.getenv("SELFHOST_ORIGIN", "").rstrip("/")
    # Optional CDN (e.g. a Bunny pull-zone at cdn.anichan.net) in front of the
    # origin. When set, /api/watch emits DIRECT cdn URLs for the heavy self-host
    # bytes (segments/audio/subtitles/fonts) instead of proxying them through this
    # box — origin/bandwidth offload. The master playlist still proxies (so its
    # in-manifest subtitle groups get stripped). Blank = proxy everything (current
    # behavior). Swapping/retiring the CDN is one DNS flip; the backend stays put.
    SELFHOST_CDN_BASE = os.getenv("SELFHOST_CDN_BASE", "").rstrip("/")
    # Video-node ingest trigger: when a user opens an anime page, fire-and-forget
    # a request here to cache the episode (+ prefetch). Empty = disabled.
    SELFHOST_INGEST_URL = os.getenv("SELFHOST_INGEST_URL", "").rstrip("/")
    SELFHOST_INGEST_TOKEN = os.getenv("SELFHOST_INGEST_TOKEN", "")  # shared secret for the trigger

    # Telegram log shipping (optional; mirrors goongle). No-op when unset.
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    TELEGRAM_TOPIC_ID = os.getenv("TELEGRAM_TOPIC_ID", "")
    SERVER_ID = os.getenv("SERVER_ID", "anime")


settings = Settings()
