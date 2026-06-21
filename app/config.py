import os


class Settings:
    # Comma-separated list, or "*" for any (dev default).
    CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*")

    # Phase 0 walking-skeleton stream: a public, CORS-friendly HLS test stream.
    # We still route it through our own /api/proxy to prove the proxy pipeline.
    TEST_STREAM = os.getenv(
        "TEST_STREAM",
        "https://test-streams.mux.dev/x36xhzz/x36xhzz.m3u8",
    )

    # Default browser-like UA used when proxying upstream hosts that fingerprint.
    USER_AGENT = os.getenv(
        "USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    )


settings = Settings()
