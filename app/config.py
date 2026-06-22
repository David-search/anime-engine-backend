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


settings = Settings()
