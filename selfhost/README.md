# selfhost — video-origin ingest & encode pipeline

Standalone scripts that run on the **GPU build/origin nodes** (vast.ai), deployed to
`/data/` there. They turn a torrent into a static HLS-at-rest package and ship it to
the serving host. **Not imported by the FastAPI app** — kept in this repo so they're
version-controlled and never lost (they previously lived only on the ephemeral nodes).

| Script | Role |
|--------|------|
| `ingest.py` | orchestrator: map (ani.zip→AniDB) → find release (AnimeTosho) → download (transmission) → build → register → push cache-state. CLI: `episode <anilist_id> <ep>`, `stats`, `evict <gb>`, `reindex`. |
| `hls_build.py` | the encoder: source → multi-quality HLS ladder (NVENC CQ / libx264 fallback) + all audio (AAC) + all subs (VTT/ASS) + fonts. `--remux-native`, `--no-nvenc`, `--cq`, `--renditions`. |
| `cache_db.py` | SQLite cache index (`/data/cache/index.db`) + LRU eviction + mapping cache. |
| `relparser.py` | release-title parser (season/episode/part extraction). |
| `at_acquire.py` | AnimeTosho acquire helper. |
| `ingest_api.py` | on-demand HTTP trigger service (DISABLED — auto-download triggers commented out). |
| `precache.py` | bulk pre-cache loop (DISABLED). |
| `stream.py` | serving/proxy helper. |
| `map_demo.py` | mapping demo/debug. |

**Canonical source.** Deploy by `scp backend/selfhost/*.py <node>:/data/`. The build
nodes need: `ffmpeg` (with `h264_nvenc`), `transmission-daemon`, `python3`, NVIDIA driver.

Docs: see `claude/self-hosted/13–18*.md` in the anime-engine-control repo.
