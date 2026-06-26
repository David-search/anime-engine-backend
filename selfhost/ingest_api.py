#!/usr/bin/env python3
"""
ingest_api.py — on-demand ingest trigger for the AniChan video node.

The clean backend calls GET /ingest?anilist_id=N&ep=E (fire-and-forget) when a
user opens an anime page; we cache that episode + a small prefetch on this
origin. Safeguards so auto-ingest can't run away:
  - shared-secret auth (X-Ingest-Token) — not a public download button
  - TWO-LANE work queue + fixed worker pool: on-demand (a viewer is watching NOW)
    is served before pre-cache, and lives in its own lane so a full pre-cache
    backlog can never reject/starve an on-demand open
  - dedup vs cached / already-queued; already-cached -> touch() (keep warm)
  - both subprocesses time-bounded; LRU-evict to CACHE_CAP_GB after each build

Endpoints: GET /ingest?anilist_id&ep[&precache=1] , GET /touch , GET /status.
Runs on :8001 (vast-mapped to a public port).
"""
import http.server, subprocess, threading, json, os, sys, urllib.parse, queue

sys.path.insert(0, "/data")
import cache_db  # noqa: E402

CAP_GB = float(os.getenv("CACHE_CAP_GB", "300"))
PREFETCH = int(os.getenv("PREFETCH", "1"))       # current ep + this many ahead
MAX_CONC = int(os.getenv("MAX_CONC", "2"))
QUEUE_MAX = int(os.getenv("QUEUE_MAX", "60"))    # pre-cache (low-priority) lane cap
HI_MAX = int(os.getenv("HI_QUEUE_MAX", "50"))    # on-demand (high-priority) lane cap
TOKEN = os.getenv("INGEST_TOKEN", "")            # shared secret; required if set
INGEST = "/data/ingest.py"
PY = sys.executable or "python3"

# Two lanes. Workers drain _hi (on-demand) before _lo (prefetch + pre-cache); the
# lanes are separate so pre-cache filling _lo can't reject an on-demand request.
_hi: "queue.Queue[tuple]" = queue.Queue(maxsize=HI_MAX)
_lo: "queue.Queue[tuple]" = queue.Queue(maxsize=QUEUE_MAX)
_inflight: set = set()                            # (aid, ep) queued or building
_lock = threading.Lock()

def _worker():
    while True:
        try:
            aid, ep = _hi.get_nowait(); q = _hi          # on-demand first
        except queue.Empty:
            try:
                aid, ep = _lo.get(timeout=0.5); q = _lo  # else a pre-cache item
            except queue.Empty:
                continue
        try:
            # DISABLED (2026-06-26): the on-demand service no longer launches downloads
            # or encodes. Builds are a manual / build-farm step — run
            # `python3 /data/ingest.py episode <aid> <ep>` by hand. The queue still
            # drains (so callers don't block) but does no work. Drop `False and` to
            # re-enable auto-ingest.
            if False and not cache_db.is_cached(aid, ep, "sub"):
                subprocess.run([PY, INGEST, "episode", str(aid), str(ep)],
                               cwd="/data", timeout=5400)
                try:
                    subprocess.run([PY, INGEST, "evict", str(CAP_GB)],
                                   cwd="/data", timeout=600)
                except subprocess.TimeoutExpired:
                    pass
        except Exception:  # noqa: BLE001
            pass
        finally:
            with _lock:
                _inflight.discard((aid, ep))
            q.task_done()

def enqueue(aid: int, ep: int, precache: bool = False):
    """The requested episode of an on-demand open goes to the high-priority lane;
    its prefetch and all pre-cache work go to the low-priority lane."""
    started, warmed = [], []
    for e in range(ep, ep + 1 + PREFETCH):
        if e < 1:
            continue
        key = (aid, e)
        if cache_db.is_cached(aid, e, "sub"):
            cache_db.touch(aid, e, "sub")         # being watched -> keep warm (anti-evict)
            warmed.append(e)
            continue
        lane = _hi if (e == ep and not precache) else _lo
        with _lock:
            if key in _inflight:
                continue
            try:
                lane.put_nowait((aid, e))         # bounded per-lane — reject when saturated
            except queue.Full:
                continue
            _inflight.add(key)
        started.append(e)
    return started, warmed

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(u.query)
        if TOKEN and self.headers.get("X-Ingest-Token", "") != TOKEN:
            return self._json({"error": "unauthorized"}, 401)
        try:
            aid = int(q["anilist_id"][0]); ep = int(q.get("ep", ["1"])[0])
        except (KeyError, ValueError, IndexError):
            aid = ep = None
        if u.path == "/ingest" and aid is not None:
            precache = q.get("precache", ["0"])[0] in ("1", "true", "yes")
            started, warmed = enqueue(aid, ep, precache)
            with _lock:
                n = len(_inflight)
            self._json({"anilist_id": aid, "ep": ep, "started": started, "warmed": warmed,
                        "precache": precache, "inflight": n})
        elif u.path == "/touch" and aid is not None:
            ok = cache_db.is_cached(aid, ep, "sub")
            if ok:
                cache_db.touch(aid, ep, "sub")
            self._json({"anilist_id": aid, "ep": ep, "touched": ok})
        elif u.path == "/status":
            with _lock:
                snap = sorted(_inflight)
            self._json({"inflight": snap, "queued_ondemand": _hi.qsize(),
                        "queued_precache": _lo.qsize(), "queued": _hi.qsize() + _lo.qsize(),
                        "cap_gb": CAP_GB})
        else:
            self._json({"error": "not found"}, 404)

    def _json(self, obj, code=200):
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def log_message(self, *a):  # quiet
        pass

if __name__ == "__main__":
    for _ in range(MAX_CONC):
        threading.Thread(target=_worker, daemon=True).start()
    print(f"ingest_api :8001 cap={CAP_GB}GB prefetch={PREFETCH} conc={MAX_CONC} "
          f"lanes(hi={HI_MAX},lo={QUEUE_MAX}) auth={'on' if TOKEN else 'OFF'}", flush=True)
    http.server.ThreadingHTTPServer(("0.0.0.0", 8001), Handler).serve_forever()
