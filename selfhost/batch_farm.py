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

def tids(): return ingest.torrent_ids()
def trinfo(tid): return ingest.tr("-t", str(tid), "-i")

def add_torrent(item):
    before = tids()
    ingest.tr("-a", item["url"], "--download-dir", LIBRARY)
    time.sleep(1.5)
    new = tids() - before
    item["tid"] = max(new) if new else None
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
        if gpu is None: cmd.append("--no-nvenc")
        else: env["CUDA_VISIBLE_DEVICES"] = str(gpu)
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

def main():
    log(f"=== batch_farm N={N} GPUS={GPUS} GPU_WORKERS_PER={GPU_WORKERS_PER} CPU_WORKERS={CPU_WORKERS} ship->{SHIP_DEST} ===")
    todo = []
    for m in airing(min(N * 2, 50)):
        if len(todo) >= N: break
        aid = m["id"]; nxt = (m.get("nextAiringEpisode") or {}).get("episode")
        ep = max(1, (nxt - 1) if nxt else (m.get("episodes") or 1))
        try: r = resolve(aid, ep)
        except Exception as e: log(f"  [resolve skip] {aid}: {e}"); r = None
        if r: todo.append(r); log(f"  [resolved] {aid} ep{ep}: {r['title'][:50]}")
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
