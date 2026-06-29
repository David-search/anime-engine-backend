#!/usr/bin/env python3
"""nzb_acquire.py — download a file from Usenet via NZBGet, given an AnimeTosho NZB URL.

Reusable by the farm: download_nzb(nzb_url, name, expected_size) -> local video path (or None).

Robustness notes (learned the hard way):
- NZBGet's daemon sometimes dies right after post-processing, before MOVING the file from
  InterDir to DestDir. So we scan BOTH dirs and accept a file as soon as it's complete:
  size stable across two polls AND (if given) >= 90% of the dump's expected size.
- The daemon is started detached (new session) so the caller's SSH close can't SIGHUP it,
  and it's re-checked/restarted if it has died.
- DupeCheck must be OFF in nzbget.conf (else a re-requested NZB is silently skipped).
"""
import os, sys, time, subprocess, urllib.request, glob, shutil

CONF = os.getenv("NZBGET_CONF", "/data/nzbget.conf")
SCAN_DIRS = [os.getenv("NZBGET_INTER", "/data/nzbget/inter"),
             os.getenv("NZBGET_COMPLETED", "/data/nzbget/completed")]
STAGING = os.getenv("NZBGET_STAGING", "/data/staging")
MIN_VIDEO = 20_000_000

def _nz(*args):
    return subprocess.run(["nzbget", "-c", CONF, *args], capture_output=True, text=True)

def _alive():
    return bool(subprocess.run(["pgrep", "-x", "nzbget"], capture_output=True, text=True).stdout.strip())

def ensure_daemon():
    if _alive():
        return True
    subprocess.Popen(["nzbget", "-c", CONF, "-D"], stdout=subprocess.DEVNULL,
                     stderr=subprocess.DEVNULL, start_new_session=True)  # detach: survive SIGHUP
    time.sleep(4)
    return _alive()

def _valid_video(path):
    """Accept a candidate only if ffprobe reads a real duration with NO errors. Guards against
    sparse/half-assembled files (NZBGet with DirectWrite, or a file grabbed before par-repair
    finished): a tail-truncated mkv still has its duration in the header but ffprobe emits an
    EBML error on stderr, so we require clean stderr + duration > 30s."""
    try:
        r = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration",
                            "-of", "default=nk=1:nw=1", path], capture_output=True, text=True, timeout=40)
        dur = float((r.stdout or "0").strip() or 0)
        return r.returncode == 0 and not r.stderr.strip() and dur > 30
    except Exception:
        return False

def _videos_for(base):
    """Videos under THIS download's own NZBGet output dir (inter/<base>.#N or completed/<base>).
    Keying on a unique per-item base name makes concurrent download_nzb() calls race-free."""
    out = {}
    for d in SCAN_DIRS:
        for ext in ("*.mkv", "*.mp4"):
            for p in glob.glob(os.path.join(d, glob.escape(base) + "*", "**", ext), recursive=True):
                try:
                    sz = os.path.getsize(p)
                    if sz > MIN_VIDEO:
                        out[p] = sz
                except OSError:
                    pass
    return out

def download_nzb(nzb_url, name, expected_size=0, timeout=1800, tag=""):
    """Fetch the NZB, hand it to NZBGet, wait for the COMPLETE video file, return its path.
    Complete = size stable across two 5s polls AND (if expected_size given) >= 90% of it.
    `tag` (e.g. "aid_ep") gives the NZB a unique collection name so parallel downloads don't
    collide on detection."""
    ensure_daemon()
    base = "q_" + ("".join(c for c in str(tag) if c.isalnum() or c == "_") or str(abs(hash(name)) % 10**9))
    nzb_path = f"/data/{base}.nzb"
    req = urllib.request.Request(nzb_url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://animetosho.org/"})
    with urllib.request.urlopen(req, timeout=60) as r, open(nzb_path, "wb") as f:
        f.write(r.read())
    _nz("-A", nzb_path)
    deadline = time.time() + timeout
    prev = {}
    while time.time() < deadline:
        if not _alive():
            ensure_daemon()                       # restart if it died (file may already be complete)
        new = _videos_for(base)
        for p, s in new.items():
            stable = prev.get(p) == s and s > MIN_VIDEO
            big_enough = expected_size <= 0 or s >= 0.9 * expected_size
            if stable and big_enough and _valid_video(p):   # ffprobe-validate: reject sparse/truncated
                try: os.remove(nzb_path)
                except OSError: pass
                # move out of NZBGet's inter/completed churn so the encoder reads a stable copy
                os.makedirs(STAGING, exist_ok=True)
                # prefix with the per-item base (q_<tag>, unique per aid_ep) so concurrent
                # downloads can't collide on a same-millisecond staging filename
                dst = os.path.join(STAGING, f"{base}_{int(time.time()*1000)}_{os.path.basename(p)}")
                try:
                    shutil.move(p, dst)
                    parent = os.path.dirname(p)
                    if "/inter/" in parent or "/completed/" in parent:
                        shutil.rmtree(parent, ignore_errors=True)
                    return dst
                except OSError:
                    return p
        prev = new
        time.sleep(5)
    try: os.remove(nzb_path)   # timeout: don't leak the queued .nzb in /data (nzbget already ingested it)
    except OSError: pass
    return None

if __name__ == "__main__":   # manual: nzb_acquire.py <nzb_url> <name> [expected_size]
    p = download_nzb(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "manual",
                     int(sys.argv[3]) if len(sys.argv) > 3 else 0)
    print(p or "FAILED")
