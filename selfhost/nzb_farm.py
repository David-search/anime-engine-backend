#!/usr/bin/env python3
"""nzb_farm.py — continuous BACK-CATALOG build farm over the NEW pipeline:

  dump_resolver todo (NZB-first) -> Usenet download (NZBGet) -> Y-mode encode -> ship.

Download is sequential (NZBGet is fast, ~28+ MB/s; one collection at a time is plenty),
encode (GPU NVENC + CPU libx264) is the bottleneck and runs in parallel, fed via a
BOUNDED queue so disk stays bounded (ship-and-delete). Torrent-source items are skipped
here (well-seeded/airing content stays on the transmission-based batch_farm).

Env: TODO_FILE, NGPU, GPU_WORKERS_PER, CPU_WORKERS, CPU_PRESET, MAXQ, LIMIT,
     SHIP_HOST, SHIP_PORT, SHIP_DEST.
"""
import os, sys, json, time, queue, threading, subprocess, shlex
sys.path.insert(0, "/data")
import nzb_acquire, ingest, dump_resolver

TODO = os.getenv("TODO_FILE", "/data/todo_dump.jsonl")
HLS = "/data/hls_build.py"
NGPU = int(os.getenv("NGPU", "4"))
GPU_WORKERS_PER = int(os.getenv("GPU_WORKERS_PER", "1"))
CPU_WORKERS = int(os.getenv("CPU_WORKERS", "2"))
CPU_PRESET = os.getenv("CPU_PRESET", "veryfast")
MAXQ = int(os.getenv("MAXQ", "6"))
DL_THREADS = int(os.getenv("DL_THREADS", "8"))   # parallel download workers (keep encoders fed)
LIMIT = int(os.getenv("LIMIT", "0"))
SHIP_HOST = os.getenv("SHIP_HOST", "")
SHIP_PORT = os.getenv("SHIP_PORT", "22")
SHIP_DEST = os.getenv("SHIP_DEST", "/data/ship")
DONE_LEDGER = os.getenv("DONE_LEDGER", "/data/done.jsonl")   # resumability: skip already-built eps
CALLBACK_URL = os.getenv("CALLBACK_URL", "")                 # backend /cache-state (coverage badges)
CALLBACK_TOKEN = os.getenv("CALLBACK_TOKEN", "")

enc_q = queue.Queue(maxsize=MAXQ)
results = []; rlock = threading.Lock(); stop = threading.Event()
def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)

import urllib.request
done_set = set(); _aid_done0 = {}; _aid_dub0 = {}
if os.path.exists(DONE_LEDGER):
    for _l in open(DONE_LEDGER):
        try:
            _d = json.loads(_l); done_set.add((_d["aid"], _d["ep"]))
            _aid_done0.setdefault(_d["aid"], set()).add(_d["ep"])
            if _d.get("dub"): _aid_dub0.setdefault(_d["aid"], set()).add(_d["ep"])
        except Exception: pass
done_lock = threading.Lock(); aid_done = _aid_done0; aid_dub = _aid_dub0

def mark_done(aid, ep, has_dub=False):
    """Append to the resume-ledger and (if configured) POST coverage to the backend. Every built
    episode is 'sub' (original audio + subtitles); additionally 'dub' when the package carries an
    English audio track (we keep JP+EN). The backend merges, so coverage never regresses on resume."""
    with done_lock:
        done_set.add((aid, ep))
        try:
            with open(DONE_LEDGER, "a") as f: f.write(json.dumps({"aid": aid, "ep": ep, "dub": has_dub}) + "\n")
        except Exception: pass
        aid_done.setdefault(aid, set()).add(ep); eps = sorted(aid_done[aid])
        if has_dub: aid_dub.setdefault(aid, set()).add(ep)
        deps = sorted(aid_dub.get(aid, set()))
    if CALLBACK_URL:
        try:
            body = json.dumps({"anilist_id": aid, "cached": {"sub": eps, "dub": deps}, "total_eps": None}).encode()
            req = urllib.request.Request(CALLBACK_URL.rstrip("/") + "/cache-state", data=body,
                  headers={"Content-Type": "application/json", "X-Ingest-Token": CALLBACK_TOKEN})
            urllib.request.urlopen(req, timeout=15).read()
        except Exception as e:
            log(f"  [callback err] {aid}: {e}")

_live_cache = {}; _live_lock = threading.Lock()
def _live_items(aid):
    """LIVE re-resolve an anime against CURRENT Nyaa seeders (cached per-anime + serialized via
    one global lock so we respect rate-limits). The frozen AnimeTosho dump carries stale/dead
    torrent picks; this finds the well-seeded releases that are alive right now."""
    with _live_lock:
        if aid not in _live_cache:
            try:
                items, _mp = dump_resolver.resolve_anime_live(aid)
                _live_cache[aid] = items or []
                log(f"  [live-resolve] {aid}: {len(_live_cache[aid])} seeded release(s)")
            except Exception as e:
                log(f"  [live-resolve ERR] {aid}: {e}"); _live_cache[aid] = []
        return _live_cache[aid]

def live_fallback(it):
    """Dead/stale dump-torrent -> re-resolve THIS ep against live Nyaa seeders and download the
    seeded release (a single, or extract just this ep from a well-seeded complete pack)."""
    aid, ep = it["aid"], it.get("ep")
    if ep is None: return None, None
    items = _live_items(aid)
    for r in items:                               # 1) a single seeded release for this exact ep
        if r.get("source") != "batch" and r.get("ep") == ep and (r.get("magnet") or r.get("url")):
            try:
                log(f"  [LIVE single] {aid}ep{ep} sd={r.get('seeders')}")
                tid, p = ingest.download(r.get("magnet") or r["url"], timeout_s=600)
                if p: return p, tid
            except Exception as e: log(f"  [live-single ERR] {aid}ep{ep}: {e}")
    for r in items:                               # 2) else a seeded complete pack -> extract this ep
        if r.get("source") == "batch" and ep in (r.get("eps") or []) and r.get("url"):
            try:
                log(f"  [LIVE pack] {aid}ep{ep} <- {str(r.get('name','pack'))[:30]}")
                for _e, path in ingest.download_batch_multi(r["url"], [(ep, r.get("season", 1))]):
                    return path, None
            except Exception as e: log(f"  [live-pack ERR] {aid}ep{ep}: {e}")
    return None, None

def acquire(it):
    """NZB primary -> torrent FALLBACK (NZBs with dead articles; a 2nd Usenet backbone
    can't help — non-Omicron backbones carry 0% of AnimeTosho). Torrent-source items go
    straight to transmission. Returns (filepath, tid); tid set for transmission downloads."""
    if it.get("source") == "nzb":
        f = None
        try:
            f = nzb_acquire.download_nzb(it["url"], it["name"], it.get("size", 0),
                                        timeout=1000, tag=f"{it['aid']}_{it.get('ep')}")
        except Exception as e:
            log(f"  [nzb ERR] {it['aid']}ep{it['ep']}: {e}")
        if f:
            return f, None
        if it.get("magnet"):                    # NZB missing/incomplete -> torrent fallback
            log(f"  [nzb-miss -> torrent fallback] {it['aid']}ep{it['ep']}")
            try:
                tid, p = ingest.download(it["magnet"], timeout_s=600)
                if p: return p, tid                  # dead torrent returns None -> fall to live_fallback
            except Exception as e:
                log(f"  [torrent-fallback ERR] {it['aid']}ep{it['ep']}: {e}")
        return live_fallback(it)                 # NZB dead + dump-torrent dead -> live Nyaa
    try:                                        # torrent-source item
        tid, p = ingest.download(it.get("magnet") or it["url"], timeout_s=600)
        if p: return p, tid                          # dead torrent returns None -> fall to live_fallback
    except Exception as e:
        log(f"  [torrent dead] {it['aid']}ep{it['ep']}: {e}")
    if it.get("nzb_fallback"):                   # dead torrent -> Usenet reliability backstop
        gb = round((it.get("nzb_fallback_size") or 0) / 1e9, 1)
        log(f"  [torrent-dead -> NZB fallback] {it['aid']}ep{it['ep']} ({gb}GB, will re-encode to shrink)")
        try:
            f = nzb_acquire.download_nzb(it["nzb_fallback"], it.get("nzb_fallback_name", "fb"),
                                         it.get("nzb_fallback_size", 0), timeout=1500,
                                         tag=f"{it['aid']}_{it.get('ep')}_fb")
            if f:
                it["reencode"] = True            # bloated fallback -> full re-encode, NOT remux
                return f, None
        except Exception as e:
            log(f"  [nzb-fallback ERR] {it['aid']}ep{it['ep']}: {e}")
    return live_fallback(it)                      # dump-torrent dead -> live Nyaa re-resolve

def process_item(it):
    """Acquire one todo item (download) and feed the encode queue. Safe to run from many
    downloader threads concurrently (download_nzb is keyed per aid_ep; transmission per tid)."""
    if it.get("source") == "batch":              # batch-only title: extract each ep from one pack
        with done_lock:                          # read done_set under the same lock mark_done writes it
            it["eps"] = [ep for ep in it["eps"] if (it["aid"], ep) not in done_set]   # resume
        if not it["eps"]: return
        eps_seasons = [(ep, it.get("season", 1)) for ep in it["eps"]]
        log(f"  [batch] {it['aid']} {len(eps_seasons)} eps from {it['name'][:40]}")
        try:
            for ep, path in ingest.download_batch_multi(it["url"], eps_seasons):
                sub = {"aid": it["aid"], "ep": ep, "title": it.get("title", "")}
                log(f"    [batch-extract] {it['aid']}ep{ep} -> {os.path.basename(path)[:34]}")
                enc_q.put((sub, path, None))      # staged file -> rm after encode
        except Exception as e:
            log(f"  [batch ERR] {it['aid']}: {e}")
        return
    with done_lock:
        if (it["aid"], it.get("ep")) in done_set: return   # resume: already built
    t0 = time.time()
    f, tid = acquire(it)
    if not f:
        log(f"  [dl FAIL] {it['aid']}ep{it['ep']}"); return
    log(f"  [dl ok] {it['aid']}ep{it['ep']} {round(time.time()-t0)}s {it['source']} -> {os.path.basename(f)[:38]}")
    enc_q.put((it, f, tid))                       # blocks when full -> bounds disk (backpressure)

def downloader(items):
    """DL_THREADS parallel download workers — keeps nzbget continuously busy + downloads
    torrents alongside, so the encoders never starve (the real throughput lever).
    NZB-FIRST: process reliable seeder-independent NZBs before slow/dead torrents+batches,
    so dead old torrents can't hog the threads and starve the encoders at the start."""
    items = sorted(items, key=lambda it: {"nzb": 0, "torrent": 1, "batch": 2}.get(it.get("source"), 3))
    item_q = queue.Queue()
    for it in items: item_q.put(it)
    def worker():
        while not stop.is_set():
            try: it = item_q.get_nowait()
            except queue.Empty: break
            try: process_item(it)
            except Exception as e: log(f"  [dl worker ERR] {it.get('aid')}: {e}")
    dls = [threading.Thread(target=worker, daemon=True) for _ in range(DL_THREADS)]
    for t in dls: t.start()
    for t in dls: t.join()
    for _ in range(NGPU * GPU_WORKERS_PER + CPU_WORKERS):
        enc_q.put(None)

def encode_worker(name, gpu):
    while not stop.is_set():
        item = enc_q.get()
        if item is None: break
        it, src, tid = item
        out = f"/data/cache/{it['aid']}/{it['ep']}/sub"
        subprocess.run(["rm", "-rf", out])
        cmd = ["python3", HLS, src, out, "--renditions", "720,480"]
        if not it.get("reencode"):               # sane source -> Y-mode remux; bloated NZB fallback -> full re-encode (shrinks the bloat)
            cmd.append("--remux-native")
        env = dict(os.environ)
        if gpu is None:
            cmd.append("--no-nvenc"); env["HLS_X264_PRESET"] = CPU_PRESET
        else:
            env["CUDA_VISIBLE_DEVICES"] = str(gpu)
        t0 = time.time(); p = subprocess.run(cmd, capture_output=True, text=True, env=env); dt = time.time() - t0
        ok = p.returncode == 0
        has_dub = False
        if ok:
            try:                       # hls_build prints a JSON report; dub = an English audio track survived the JP+EN filter
                rep = json.loads(p.stdout)
                has_dub = any((a.get("lang") or "").lower().startswith("en") for a in rep.get("audio", []))
            except Exception: pass
        ship_s = 0.0; ship_ok = True
        if ok:
            dest = f"{SHIP_DEST}/{it['aid']}/{it['ep']}/sub"; ts = time.time()
            if SHIP_HOST:
                # tar-stream over ONE ssh connection. An HLS package is thousands of tiny segment
                # files; rsync's per-file round-trips made shipping take up to ~1000s. One tar pipe
                # ships them in seconds. rm+mkdir first so a re-shipped episode has no stale segments.
                remote = f"rm -rf {dest} && mkdir -p {dest} && tar -C {dest} -xf -"
                ship_cmd = (f"tar -C {shlex.quote(out)} -cf - . | "
                            f"ssh -p {SHIP_PORT} -o StrictHostKeyChecking=no -o BatchMode=yes "
                            f"{SHIP_HOST} {shlex.quote(remote)}")
                rc = subprocess.run(ship_cmd, shell=True, capture_output=True)
            else:
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                rc = subprocess.run(["rsync", "-a", out + "/", dest + "/"], capture_output=True)
            ship_ok = (rc.returncode == 0)                       # don't mark "done" if the ship FAILED
            ship_s = time.time() - ts
        subprocess.run(["rm", "-rf", out])                       # ALWAYS clean cache dir (success OR fail) -> no disk leak
        if tid is not None:                                       # torrent: remove torrent + its data
            try: ingest.tr("-t", str(tid), "--remove-and-delete")
            except Exception: pass
        else:
            subprocess.run(["rm", "-rf", src])                   # staged NZB file
        built = ok and ship_ok                                   # only "done" if encoded AND shipped
        if built:
            mark_done(it["aid"], it["ep"], has_dub)           # resume-ledger + sub/dub coverage callback
        with rlock:
            results.append({"aid": it["aid"], "ep": it["ep"], "ok": built, "enc_s": round(dt, 1), "ship_s": round(ship_s, 1)})
        status = "OK" if built else ("SHIP-FAIL" if (ok and not ship_ok) else "FAIL " + p.stderr[-120:])
        log(f"  [done] {name} {it['aid']}ep{it['ep']} enc {round(dt)}s ship {round(ship_s)}s {status}")

def main():
    items = [json.loads(l) for l in open(TODO) if l.strip()]   # all sources: nzb, torrent, batch
    if LIMIT: items = items[:LIMIT]
    n_nzb = sum(1 for i in items if i.get("source") == "nzb")
    n_tor = sum(1 for i in items if i.get("source") == "torrent")
    n_bat = sum(1 for i in items if i.get("source") == "batch")
    bat_eps = sum(len(i.get("eps", [])) for i in items if i.get("source") == "batch")
    log(f"nzb_farm: {len(items)} items ({n_nzb} nzb, {n_tor} torrent, {n_bat} batch->{bat_eps} eps) | "
        f"{NGPU}x{GPU_WORKERS_PER} GPU + {CPU_WORKERS} CPU workers | MAXQ={MAXQ} -> {SHIP_DEST}")
    nzb_acquire.ensure_daemon()
    t_start = time.time()
    workers = []
    for g in range(NGPU):
        for k in range(GPU_WORKERS_PER):
            t = threading.Thread(target=encode_worker, args=(f"gpu{g}.{k}", g), daemon=True); t.start(); workers.append(t)
    for k in range(CPU_WORKERS):
        t = threading.Thread(target=encode_worker, args=(f"cpu{k}", None), daemon=True); t.start(); workers.append(t)
    dl = threading.Thread(target=downloader, args=(items,), daemon=True); dl.start()
    dl.join()
    for t in workers: t.join()
    ok = sum(1 for r in results if r["ok"]); wall = time.time() - t_start
    rate = round(len(results) / (wall / 3600), 1) if wall else 0
    log(f"DONE: {ok}/{len(results)} ok in {round(wall)}s -> {rate} eps/hr")
    print("SUMMARY " + json.dumps({"done": len(results), "ok": ok, "wall_s": round(wall), "eps_hr": rate}))

if __name__ == "__main__":
    main()
