#!/usr/bin/env python3
"""
stream.py — progressive torrent-stream-on-play (Phase 3).

Streams a torrent file to ffmpeg AS IT DOWNLOADS, producing HLS EVENT segments so
the first viewer of a cold episode watches within seconds. Webtor.io shape:

  libtorrent (sequential + set_piece_deadline)
     -> HTTP-Range seeder (serves the still-downloading file; blocks on missing
        pieces, prioritises the requested byte range — so ffmpeg can SEEK to read
        the MKV cues-at-end, fixing the non-seekable-pipe problem)
     -> ffmpeg -seekable 1 (-c:v copy for H.264, NVENC for HEVC/10-bit)
     -> HLS EVENT playlist + segments into the cache dir
     -> on completion: append #EXT-X-ENDLIST (EVENT -> VOD).

CLI: stream.py <torrent_url|magnet> <out_dir>
"""
import libtorrent as lt
import http.server, threading, subprocess, os, sys, time, json, re, urllib.request

SAVE_DIR = os.getenv("STREAM_TMP", "/data/stream_tmp")
SEED_PORT = int(os.getenv("STREAM_SEED_PORT", "8090"))
PIECE_TIMEOUT = 180

class Streamer:
    def __init__(self, source):
        self.ses = lt.session({"listen_interfaces": "0.0.0.0:6881,0.0.0.0:6891"})
        os.makedirs(SAVE_DIR, exist_ok=True)
        atp = (lt.parse_magnet_uri(source) if source.startswith("magnet:")
               else lt.add_torrent_params())
        if not source.startswith("magnet:"):
            data = urllib.request.urlopen(source, timeout=25).read()
            atp.ti = lt.torrent_info(lt.bdecode(data))
        atp.save_path = SAVE_DIR
        self.h = self.ses.add_torrent(atp)
        print("[stream] fetching metadata…", flush=True)
        while not self.h.status().has_metadata:
            time.sleep(0.2)
        self.ti = self.h.torrent_file()
        self.psize = self.ti.piece_length()
        fs = self.ti.files()
        self.fidx = max(range(self.ti.num_files()), key=lambda i: fs.file_size(i))
        self.foffset = fs.file_offset(self.fidx)
        self.fsize = fs.file_size(self.fidx)
        self.fpath = os.path.join(SAVE_DIR, fs.file_path(self.fidx))
        pr = [0] * self.ti.num_files(); pr[self.fidx] = 4
        self.h.prioritize_files(pr)
        self.h.set_sequential_download(True)
        print(f"[stream] file: {os.path.basename(self.fpath)} ({self.fsize//1048576}MB, "
              f"{self.ti.num_pieces()} pieces × {self.psize//1024}KB)", flush=True)

    def _wait(self, abs_start, length):
        p0 = abs_start // self.psize
        p1 = (abs_start + length - 1) // self.psize
        deadline = 0
        for p in range(p0, p1 + 1):
            self.h.piece_priority(p, 7)
            self.h.set_piece_deadline(p, deadline); deadline += 50
        t0 = time.time()
        while time.time() - t0 < PIECE_TIMEOUT:
            if all(self.h.have_piece(p) for p in range(p0, p1 + 1)):
                return True
            time.sleep(0.05)
        return False

    def read(self, file_start, length):
        length = min(length, self.fsize - file_start)
        if length <= 0:
            return b""
        if not self._wait(self.foffset + file_start, length):
            return None
        with open(self.fpath, "rb") as f:
            f.seek(file_start)
            return f.read(length)

    def progress(self):
        s = self.h.status()
        return s.progress, s.num_peers, int(s.download_rate / 1024)

def make_handler(streamer):
    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            s = streamer
            rng = self.headers.get("Range")
            if rng and (m := re.match(r"bytes=(\d+)-(\d*)", rng)):
                start = int(m.group(1)); end = int(m.group(2)) if m.group(2) else s.fsize - 1
                code = 206
            else:
                start, end = 0, s.fsize - 1; code = 200
            end = min(end, s.fsize - 1)
            self.send_response(code)
            self.send_header("Content-Type", "video/x-matroska")
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(end - start + 1))
            if code == 206:
                self.send_header("Content-Range", f"bytes {start}-{end}/{s.fsize}")
            self.end_headers()
            pos = start
            while pos <= end:
                chunk = s.read(pos, min(s.psize, end - pos + 1))
                if not chunk:
                    break
                try:
                    self.wfile.write(chunk)
                except (BrokenPipeError, ConnectionResetError):
                    break
                pos += len(chunk)
        def do_HEAD(self):
            self.send_response(200)
            self.send_header("Content-Length", str(streamer.fsize))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
        def log_message(self, *a):
            pass
    return H

def codec(url):
    try:
        out = subprocess.check_output(
            ["ffprobe", "-v", "error", "-seekable", "1", "-select_streams", "v:0",
             "-show_entries", "stream=codec_name,pix_fmt", "-of", "json", url],
            timeout=90).decode()
        v = json.loads(out)["streams"][0]
        return v.get("codec_name"), v.get("pix_fmt")
    except Exception:  # noqa: BLE001
        return None, None

def main():
    source, out = sys.argv[1], sys.argv[2]
    v0 = os.path.join(out, "v0"); os.makedirs(v0, exist_ok=True)
    st = Streamer(source)
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", SEED_PORT), make_handler(st))
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{SEED_PORT}/file"

    cn, pf = codec(url)
    is_h264_8bit = cn == "h264" and "p10" not in (pf or "")
    print(f"[stream] codec={cn} pix={pf} -> {'REMUX (copy)' if is_h264_8bit else 'NVENC transcode'}", flush=True)
    venc = ["-c:v", "copy"] if is_h264_8bit else \
           ["-vf", "scale=-2:1080", "-c:v", "h264_nvenc", "-preset", "p5", "-b:v", "4000k",
            "-pix_fmt", "yuv420p", "-profile:v", "high"]

    ff = subprocess.Popen(
        ["ffmpeg", "-v", "warning", "-seekable", "1", "-i", url,
         "-map", "0:v:0", "-map", "0:a:0?", *venc, "-c:a", "aac", "-b:a", "128k", "-sn",
         "-f", "hls", "-hls_time", "6", "-hls_playlist_type", "event",
         "-hls_flags", "independent_segments+append_list", "-hls_segment_type", "mpegts",
         "-hls_segment_filename", os.path.join(v0, "seg%03d.ts"), os.path.join(v0, "index.m3u8")])

    with open(os.path.join(out, "master.m3u8"), "w") as f:
        f.write('#EXTM3U\n#EXT-X-VERSION:6\n'
                '#EXT-X-STREAM-INF:BANDWIDTH=4000000,RESOLUTION=1920x1080,CODECS="avc1.640028,mp4a.40.2"\n'
                'v0/index.m3u8\n')

    t0 = time.time(); first_seg = None
    while ff.poll() is None:
        n = len([x for x in os.listdir(v0) if x.endswith(".ts")])
        if n and first_seg is None:
            first_seg = time.time() - t0
            print(f"[stream] FIRST SEGMENT at {first_seg:.1f}s — playable now", flush=True)
        prog, peers, rate = st.progress()
        print(f"[stream] dl {prog*100:.0f}% {rate}KB/s {peers}p · {n} segments", flush=True)
        time.sleep(5)
    # EVENT -> VOD
    idx = os.path.join(v0, "index.m3u8")
    if os.path.exists(idx):
        with open(idx) as f:
            body = f.read()
        if "#EXT-X-ENDLIST" not in body:
            with open(idx, "a") as f:
                f.write("#EXT-X-ENDLIST\n")
    print(f"[stream] done — first-seg {first_seg}s, total {time.time()-t0:.0f}s, "
          f"{len(os.listdir(v0))-1} segments", flush=True)

if __name__ == "__main__":
    main()
