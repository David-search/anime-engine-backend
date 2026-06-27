#!/usr/bin/env python3
"""
batch_farm.py — pipelined parallel build-farm harness (full pipeline + ship-and-delete).

FULL pipeline at max throughput on one box:
  resolve N popular airing episodes -> add ALL torrents at once (parallel download)
  -> SEPARATE encode pool consumes downloads as they finish (download-ahead pipelining):
       per-GPU-pinned NVENC workers (CUDA_VISIBLE_DEVICES) x GPU_WORKERS_PER per card
       + CPU libx264 workers on the spare cores (--no-nvenc)
  -> each finished episode is SHIPPED to the (mock) host (rsync) then DELETED locally
     (source torrent removed too) so disk stays bounded.
All builds "Y" mode (remux 1080 + encode 720/480).

Reports: peak/avg download Mbps, GPU vs CPU eps + times, ship time, PEAK DISK (proves
ship-and-delete bounds it), worker idle %, and overall eps/hr -> download- or encode-bound.

Env: N (15), GPU_WORKERS_PER (2), CPU_WORKERS (4), DL_TIMEOUT (1800),
     SHIP_DEST (/data/mock_host), NGPU (4).
"""
import sys, os, time, json, threading, queue, subprocess, re, shutil
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, "/data")
import ingest

LIBRARY = ingest.LIBRARY
NGPU = int(os.getenv("NGPU", "4"))
GPUS = list(range(NGPU))
GPU_WORKERS_PER = int(os.getenv("GPU_WORKERS_PER", "2"))
CPU_WORKERS = int(os.getenv("CPU_WORKERS", "4"))
N = int(os.getenv("N", "15"))
DL_TIMEOUT = int(os.getenv("DL_TIMEOUT", "1800"))
HLS = "/data/hls_build.py"
SHIP_DEST = os.getenv("SHIP_DEST", "/data/mock_host")   # mock "host" — rsync target for ship-and-delete
MAX_INFLIGHT = int(os.getenv("MAX_INFLIGHT", "40"))     # continuous mode: target concurrent downloads
DURATION = int(os.getenv("DURATION", "900"))            # continuous mode: measured run length (s)
RESOLVERS = int(os.getenv("RESOLVERS", "1"))            # continuous mode: parallel resolve threads

def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)

def airing(n):
    q = ("query($n:Int){Page(perPage:$n){media(type:ANIME,status:RELEASING,sort:TRENDING_DESC){"
         "id episodes nextAiringEpisode{episode} title{romaji english}}}}")
    d = ingest.hj("https://graphql.anilist.co", json.dumps({"query": q, "variables": {"n": n}}).encode())
    return d["data"]["Page"]["media"]

def resolve(aid, ep):
    mp = ingest.map_anidb(aid, ep)
    if not mp.get("anidb_id"): return None
    ep2eid = {v: k for k, v in mp["eid_to_ep"].items()}
    eids = [ep2eid[ep]] if ep in ep2eid else None
    rels = ingest.find_releases(mp["anidb_id"], mp["romaji"], mp["english"], eids=eids)
    sel, conf = ingest.select_release(rels, ep, mp, allow_hevc=False)  # H.264 only (remuxable)
    if not sel or conf == "batch": return None
    return {"aid": aid, "ep": ep, "url": sel["torrent_url"], "title": sel["title"]}

enc_q = queue.Queue()
results = []; res_lock = threading.Lock()
dl_done = 0; dl_lock = threading.Lock()
bw_samples = []; idle_time = {}; stop = threading.Event(); peak_disk = 0
inflight = {}; inflight_lock = threading.Lock()          # tid -> item (continuous mode)
_slate = []; _slate_i = 0; _slate_lock = threading.Lock()

def tids(): return ingest.torrent_ids()
def trinfo(tid): return ingest.tr("-t", str(tid), "-i")

_add_lock = threading.Lock()
def add_torrent(item):
    with _add_lock:                                  # serialize adds so tid-diffing is unambiguous
        before = tids()
        ingest.tr("-a", item["url"], "--download-dir", LIBRARY)
        for _ in range(20):                          # poll up to ~10s for the new torrent id
            time.sleep(0.5)
            new = tids() - before
            if new:
                item["tid"] = max(new); return item
    # already-present torrent (no new id) -> match by release name
    key = re.sub(r"[^a-z0-9]", "", item["title"].lower())[:24]
    for tid in tids():
        nm = re.search(r"Name:\s*(.+)", trinfo(tid))
        if nm and key and key in re.sub(r"[^a-z0-9]", "", nm.group(1).lower()):
            item["tid"] = tid; return item
    item["tid"] = None
    return item

def file_for(tid):
    info = trinfo(tid)
    name = re.search(r"Name:\s*(.+)", info); loc = re.search(r"Location:\s*(.+)", info)
    if not (name and loc): return None
    path = os.path.join(loc.group(1).strip(), name.group(1).strip())
    if os.path.isdir(path):
        mkvs = [os.path.join(r, f) for r, _, fs in os.walk(path) for f in fs if f.lower().endswith(".mkv")]
        path = max(mkvs, key=os.path.getsize) if mkvs else path
    return path if os.path.exists(path) else None

def pct(tid):
    m = re.search(r"Percent Done:\s*([\d.]+)%", trinfo(tid)); return float(m.group(1)) if m else 0.0

def dl_watcher(items):
    global dl_done
    pending = {it["tid"]: it for it in items if it.get("tid")}
    t0 = time.time()
    while pending and not stop.is_set() and time.time() - t0 < DL_TIMEOUT:
        for tid in list(pending):
            if pct(tid) >= 100:
                it = pending.pop(tid); f = file_for(tid)
                if f:
                    it["file"] = f; enc_q.put(it)
                    with dl_lock: dl_done += 1
                    log(f"  [dl done] {os.path.basename(f)[:55]}  ({dl_done} downloaded)")
        time.sleep(5)
    for _ in range(GPU_WORKERS_PER * len(GPUS) + CPU_WORKERS): enc_q.put(None)

def resolver_thread(deadline, name):
    """Continuously resolve airing episodes + add torrents, keeping <=MAX_INFLIGHT
    downloads in flight, so the pipe stays full while encoders drain. Serial per
    thread (gentle on AniList/AnimeTosho); RESOLVERS threads run in parallel."""
    global _slate_i
    while time.time() < deadline and not stop.is_set():
        with inflight_lock: n = len(inflight)
        if n >= MAX_INFLIGHT:
            time.sleep(2); continue
        with _slate_lock:
            if _slate_i >= len(_slate):
                i = None
            else:
                i = _slate_i; _slate_i += 1
        if i is None:
            time.sleep(3); continue                      # slate exhausted; let encoders drain
        m = _slate[i]
        aid = m["id"]; nxt = (m.get("nextAiringEpisode") or {}).get("episode")
        ep = max(1, (nxt - 1) if nxt else (m.get("episodes") or 1))
        try: r = resolve(aid, ep)
        except Exception as e: log(f"  [resolve skip] {aid}: {e}"); continue
        if not r: continue
        add_torrent(r)
        if r.get("tid"):
            with inflight_lock: inflight[r["tid"]] = r
            log(f"  [+dl] {r['title'][:46]} (inflight {len(inflight)})")

def feeder_thread(deadline, items):
    """Add PRE-RESOLVED torrents (no API call), keeping <=MAX_INFLIGHT in flight.
    This is the decoupled hot loop: resolve already happened offline (pre_resolve.py)."""
    i = 0
    while time.time() < deadline and not stop.is_set() and i < len(items):
        with inflight_lock: n = len(inflight)
        if n >= MAX_INFLIGHT:
            time.sleep(1); continue
        it = dict(items[i]); i += 1
        add_torrent(it)
        if it.get("tid"):
            with inflight_lock: inflight[it["tid"]] = it
            log(f"  [+dl] {it['title'][:46]} (inflight {len(inflight)})")
    log(f"  [feeder] exhausted ({i} fed)")

def dl_watcher_continuous(deadline):
    global dl_done
    hard_stop = deadline + 300
    while not stop.is_set() and time.time() < hard_stop and (time.time() < deadline or inflight):
        with inflight_lock: tracked = list(inflight.items())
        for tid, it in tracked:
            if pct(tid) >= 100:
                f = file_for(tid)
                with inflight_lock: inflight.pop(tid, None)
                if f:
                    it["file"] = f; enc_q.put(it)
                    with dl_lock: dl_done += 1
                    log(f"  [dl done] {os.path.basename(f)[:46]} ({dl_done})")
        time.sleep(4)
    for _ in range(GPU_WORKERS_PER * len(GPUS) + CPU_WORKERS): enc_q.put(None)

def sampler():
    global peak_disk
    while not stop.is_set():
        for line in ingest.tr("-l").splitlines():
            if line.strip().startswith("Sum:"):
                nums = re.findall(r"[\d.]+", line)
                if nums: bw_samples.append(float(nums[-1]) * 8 / 1000)   # aggregate down kB/s -> Mbps
        try: peak_disk = max(peak_disk, shutil.disk_usage("/data").used)
        except Exception: pass
        time.sleep(3)

def encode_worker(name, gpu):
    idle_time[name] = 0.0
    while not stop.is_set():
        tw = time.time(); item = enc_q.get(); idle_time[name] += time.time() - tw
        if item is None: break
        out = f"/data/cache/{item['aid']}/{item['ep']}/sub"
        subprocess.run(["rm", "-rf", out])
        cmd = ["python3", HLS, item["file"], out, "--remux-native", "--renditions", "720,480"]
        env = dict(os.environ)
        if gpu is None:
            cmd.append("--no-nvenc")
            env["HLS_X264_PRESET"] = os.getenv("CPU_PRESET", "veryfast")   # fast CPU encode (slow backfired)
        else:
            env["CUDA_VISIBLE_DEVICES"] = str(gpu)
        t0 = time.time(); p = subprocess.run(cmd, capture_output=True, text=True, env=env); dt = time.time() - t0
        ok = p.returncode == 0
        size = sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(out) for f in fs) if ok and os.path.isdir(out) else 0
        ship_s = 0.0
        if ok:
            dest = f"{SHIP_DEST}/{item['aid']}/{item['ep']}/sub"
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            ts = time.time()
            subprocess.run(["rsync", "-a", out + "/", dest + "/"])   # ship to (mock) host
            ship_s = time.time() - ts
            subprocess.run(["rm", "-rf", out])                       # delete local build (now on host)
        try:                                                          # remove source torrent+data -> disk bounded
            if item.get("tid"): ingest.tr("-t", str(item["tid"]), "--remove-and-delete")
        except Exception: pass
        with res_lock:
            results.append({"aid": item["aid"], "ep": item["ep"], "worker": name,
                            "enc_s": round(dt, 1), "ship_s": round(ship_s, 1), "gb": round(size/1e9, 2), "ok": ok})
        log(f"  [done] {name} {item['aid']}ep{item['ep']} enc {round(dt)}s ship {round(ship_s)}s {'OK' if ok else 'FAIL '+p.stderr[-140:]}")

def run_continuous():
    global _slate
    log(f"CONTINUOUS: MAX_INFLIGHT={MAX_INFLIGHT} DURATION={DURATION}s RESOLVERS={RESOLVERS} "
        f"({GPU_WORKERS_PER*len(GPUS)} GPU + {CPU_WORKERS} CPU workers)")
    todo_file = os.getenv("TODO_FILE")
    t_start = time.time(); deadline = t_start + DURATION
    threading.Thread(target=sampler, daemon=True).start()
    if todo_file:
        items = [json.loads(l) for l in open(todo_file) if l.strip()]
        log(f"  feeding {len(items)} PRE-RESOLVED episodes from {todo_file} (no live resolve)")
        threading.Thread(target=feeder_thread, args=(deadline, items), daemon=True).start()
    else:
        _slate = airing(50)
        for k in range(RESOLVERS):
            threading.Thread(target=resolver_thread, args=(deadline, f"res{k}"), daemon=True).start()
    threading.Thread(target=dl_watcher_continuous, args=(deadline,), daemon=True).start()
    workers = []
    for g in GPUS:
        for w in range(GPU_WORKERS_PER):
            t = threading.Thread(target=encode_worker, args=(f"gpu{g}.{w}", g)); t.start(); workers.append(t)
    for c in range(CPU_WORKERS):
        t = threading.Thread(target=encode_worker, args=(f"cpu{c}", None)); t.start(); workers.append(t)
    for t in workers: t.join()
    stop.set(); wall = time.time() - t_start
    done = [r for r in results if r["ok"]]
    peak = max(bw_samples, default=0); avg = sum(bw_samples)/len(bw_samples) if bw_samples else 0
    gpu_done = [r for r in done if r["worker"].startswith("gpu")]; cpu_done = [r for r in done if r["worker"].startswith("cpu")]
    tot_idle = sum(idle_time.values()); tot_wt = wall*len(workers); idle_pct = 100*tot_idle/max(tot_wt, 1)
    s = {"done": len(done), "wall_s": round(wall), "eps_hr": round(len(done)/wall*3600, 1),
         "added": dl_done, "peak_down_mbps": round(peak), "avg_down_mbps": round(avg),
         "gpu_eps": len(gpu_done), "cpu_eps": len(cpu_done),
         "ship_avg_s": round(sum(r.get("ship_s", 0) for r in done)/max(len(done), 1), 1),
         "peak_disk_gb": round(peak_disk/1e9, 1),
         "worker_idle_pct": round(idle_pct), "verdict": "FEED-bound (resolve/download)" if idle_pct > 30 else "ENCODE-bound"}
    log("=" * 64)
    log(f"CONTINUOUS RESULT: {s['done']} eps in {s['wall_s']}s -> {s['eps_hr']} eps/hr SUSTAINED  (added {s['added']})")
    log(f"download: peak {s['peak_down_mbps']} Mbps, avg {s['avg_down_mbps']} Mbps")
    log(f"encode: GPU {s['gpu_eps']} | CPU {s['cpu_eps']} | ship avg {s['ship_avg_s']}s | peak disk {s['peak_disk_gb']} GB")
    log(f"encoder idle: {s['worker_idle_pct']}% -> {s['verdict']}")
    print("SUMMARY " + json.dumps(s))

def main():
    log(f"=== batch_farm N={N} GPUS={GPUS} GPU_WORKERS_PER={GPU_WORKERS_PER} CPU_WORKERS={CPU_WORKERS} ship->{SHIP_DEST} ===")
    if os.getenv("CONTINUOUS"):
        return run_continuous()
    import glob as _glob
    if os.getenv("ENCODE_ONLY"):                          # skip resolve+download: just encode existing /data/library sources
        srcs = sorted(_glob.glob(os.path.join(LIBRARY, "*.mkv")))
        todo = [{"aid": "lib", "ep": i, "file": f, "tid": None, "title": os.path.basename(f)} for i, f in enumerate(srcs)]
        log(f"ENCODE_ONLY: {len(todo)} sources in {LIBRARY}")
        if not todo: return
        t_start = time.time()
        threading.Thread(target=sampler, daemon=True).start()
        for it in todo: enc_q.put(it)
        for _ in range(GPU_WORKERS_PER * len(GPUS) + CPU_WORKERS): enc_q.put(None)
    else:
        todo = []
        for m in airing(min(N * 3, 50)):
            if len(todo) >= N: break                      # serial = gentlest on AniList/AnimeTosho (parallel hit 429s + stalls)
            aid = m["id"]; nxt = (m.get("nextAiringEpisode") or {}).get("episode")
            ep = max(1, (nxt - 1) if nxt else (m.get("episodes") or 1))
            try: r = resolve(aid, ep)
            except Exception as e: log(f"  [resolve skip] {aid}: {e}"); r = None
            if r: todo.append(r); log(f"  [resolved] {r['aid']} ep{r['ep']}: {r['title'][:50]}")
        log(f"resolved {len(todo)}/{N}")
        if not todo: return
        t_start = time.time()
        for it in todo: add_torrent(it)
        log(f"added {sum(1 for it in todo if it.get('tid'))} torrents (parallel download begins)")
        threading.Thread(target=sampler, daemon=True).start()
        threading.Thread(target=dl_watcher, args=(todo,), daemon=True).start()
    workers = []
    for g in GPUS:
        for w in range(GPU_WORKERS_PER):
            t = threading.Thread(target=encode_worker, args=(f"gpu{g}.{w}", g)); t.start(); workers.append(t)
    for c in range(CPU_WORKERS):
        t = threading.Thread(target=encode_worker, args=(f"cpu{c}", None)); t.start(); workers.append(t)
    for t in workers: t.join()
    stop.set(); wall = time.time() - t_start

    done = [r for r in results if r["ok"]]
    peak = max(bw_samples, default=0); avg = sum(bw_samples)/len(bw_samples) if bw_samples else 0
    gpu_done = [r for r in done if r["worker"].startswith("gpu")]; cpu_done = [r for r in done if r["worker"].startswith("cpu")]
    tot_idle = sum(idle_time.values()); tot_wt = wall * len(workers); idle_pct = 100*tot_idle/max(tot_wt, 1)
    s = {"done": len(done), "of": len(todo), "wall_s": round(wall), "eps_hr": round(len(done)/wall*3600, 1),
         "peak_down_mbps": round(peak), "avg_down_mbps": round(avg),
         "gpu_eps": len(gpu_done), "gpu_avg_s": round(sum(r["enc_s"] for r in gpu_done)/max(len(gpu_done), 1)),
         "cpu_eps": len(cpu_done), "cpu_avg_s": round(sum(r["enc_s"] for r in cpu_done)/max(len(cpu_done), 1)),
         "ship_avg_s": round(sum(r.get("ship_s", 0) for r in done)/max(len(done), 1), 1),
         "peak_disk_gb": round(peak_disk/1e9, 1),
         "worker_idle_pct": round(idle_pct), "verdict": "DOWNLOAD-bound" if idle_pct > 30 else "ENCODE-bound"}
    log("=" * 64)
    log(f"RESULT: {s['done']}/{s['of']} eps full pipeline in {s['wall_s']}s -> {s['eps_hr']} eps/hr")
    log(f"download (parallel): peak {s['peak_down_mbps']} Mbps, avg {s['avg_down_mbps']} Mbps")
    log(f"encode: GPU {s['gpu_eps']} eps @~{s['gpu_avg_s']}s | CPU {s['cpu_eps']} eps @~{s['cpu_avg_s']}s")
    log(f"ship (mock rsync): avg {s['ship_avg_s']}s/ep | PEAK DISK during run: {s['peak_disk_gb']} GB (ship-and-delete bounds it)")
    log(f"encoder idle: {s['worker_idle_pct']}% -> {s['verdict']}")
    print("SUMMARY " + json.dumps(s))

if __name__ == "__main__":
    main()
