from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import settings
from .proxy import router as proxy_router
from .routers import catalog, servers, sources


@asynccontextmanager
async def lifespan(app: FastAPI):
    # One shared client for AniList, provider calls, and the streaming proxy.
    app.state.http = httpx.AsyncClient(
        follow_redirects=True,
        timeout=httpx.Timeout(20.0, read=60.0),
        headers={"User-Agent": settings.USER_AGENT},
    )
    try:
        yield
    finally:
        await app.state.http.aclose()
        from .cf import manager
        await manager.close()


app = FastAPI(title="Anime Clone API", version="0.1.0", lifespan=lifespan)

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
app.include_router(sources.router, prefix="/api")
app.include_router(servers.router, prefix="/api")
app.include_router(proxy_router, prefix="/api")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/")
async def root():
    return {"name": "anime-clone-api", "version": "0.1.0", "docs": "/docs"}
