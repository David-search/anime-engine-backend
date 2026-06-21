# backend — Anime Clone API (FastAPI)

Phase 0 walking skeleton: AniList catalog, an HLS/CORS proxy, and a `/sources`
stub that serves a public test stream through the proxy. Provider extraction
(allanime → … → MegaCloud) plugs in at Phase 1 behind `app/providers/base.py`.

## Run

```bash
cd backend
./.venv/bin/uvicorn app.main:app --reload --port 8000
```

(`.venv` is Python 3.12. If missing: `python3.12 -m venv .venv && ./.venv/bin/pip install -r requirements.txt`.)

Docs at http://localhost:8000/docs

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | liveness |
| GET | `/api/catalog/trending?page=1` | AniList trending |
| GET | `/api/catalog/popular?page=1` | AniList popular |
| GET | `/api/catalog/search?q=frieren` | AniList search |
| GET | `/api/catalog/anime/{id}` | AniList detail |
| GET | `/api/sources?anime_id=&ep=1&category=sub` | resolve stream (Phase 0: test stub) |
| GET | `/api/proxy?u=<b64url>&h=<b64json>` | HLS/segment/CORS proxy |

## Layout

```
app/
  main.py            FastAPI app, CORS, shared httpx client
  config.py          env settings
  anilist.py         AniList GraphQL client (catalog)
  proxy.py           m3u8/segment proxy + build_proxy_url()
  routers/
    catalog.py       /api/catalog/*
    sources.py       /api/sources  (Phase 0 stub)
  providers/
    base.py          AnimeProvider interface + types (Phase 1 impls go here)
```
