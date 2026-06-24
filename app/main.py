import logging
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from . import db, es
from .config import settings
from .routers import auth, catalog, search, social, watch
from .telegram_logger import setup_telegram_logging

# Logs (incl. WARNING/ERROR) ship to a Telegram channel when configured (mirrors goongle).
logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(name)s · %(levelname)s · %(message)s")
for _noisy in ("httpx", "httpcore", "uvicorn.access", "elastic_transport"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)
logger = logging.getLogger("anichan")
_tg = setup_telegram_logging(level=logging.INFO)
if _tg:
    logging.getLogger().addHandler(_tg)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http = httpx.AsyncClient(
        follow_redirects=True,
        timeout=httpx.Timeout(20.0, read=60.0),
        headers={"User-Agent": settings.USER_AGENT},
    )
    # Best-effort: don't block boot if Mongo/ES are unreachable.
    try:
        await db.ensure_indexes()
    except Exception as e:  # noqa: BLE001
        logger.warning("mongo init skipped: %s", e)
    try:
        await es.ensure_index()
    except Exception as e:  # noqa: BLE001
        logger.warning("es init skipped: %s", e)
    logger.info("AniChan backend %s started", app.version)
    try:
        yield
    finally:
        await app.state.http.aclose()
        await db.close()
        await es.close()


app = FastAPI(title="AniChan API", version="0.3.0", lifespan=lifespan)

_origins = ["*"] if settings.CORS_ORIGINS.strip() == "*" else [
    o.strip() for o in settings.CORS_ORIGINS.split(",") if o.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

app.include_router(catalog.router, prefix="/api")
app.include_router(search.router, prefix="/api")
app.include_router(auth.router, prefix="/api")
app.include_router(social.router, prefix="/api")
app.include_router(watch.router, prefix="/api")


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "mongo": bool(settings.MONGO_URI),
        "elastic": bool(settings.ELASTIC_URL),
    }


@app.get("/")
async def root():
    return {"name": "anichan-api", "version": "0.3.0", "docs": "/docs"}
