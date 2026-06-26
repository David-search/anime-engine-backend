#!/usr/bin/env python3
"""
hls_build.py — doc-07 "HLS-at-rest" builder (measured prototype).

Takes one source video (MKV/MP4) and produces a multi-quality, multi-audio,
multi-subtitle HLS package on disk under <out>/ — the static files a CDN/edge
serves. ONE-TIME build per episode (remux master, NVENC ladder, extract EVERY
sub + audio track). Serving is then a pure static file read; NO per-viewer
transcode ever.

  out/{animeId}/{ep}/{cat}/
    master.m3u8         # renditions + EXT-X-MEDIA audio/subtitle groups
    v0/ index.m3u8 seg*.ts   # native res (re-encoded to CQ target; --remux-native to copy)
    v1/ ...  720      v2/ ...  480     (NVENC, built ONCE, cached static)
    a0/ a1/ ...              # one HLS-AAC rendition per audio track (sub+dub)
    subs/ <lang><n>.vtt + .m3u8 (+ original .ass)   # EVERY text subtitle track

Every rendition (native + downscaled) is re-encoded to an anime-tuned CONSTANT-QUALITY
target (NVENC CQ / libx264 CRF, with a maxrate ceiling) so each cached 1080p is a
consistent ~3 Mbps instead of passing a fat WEB-DL/BD source straight through. Pass
--remux-native to losslessly copy an H.264/8-bit source instead (opt-in). Lower
renditions always encode (NVENC, or libx264 with --no-nvenc). Every step timed; JSON
report printed (sizes, mbps, realtime factors, segment stats).
"""
import json, os, shutil, subprocess, sys, time, argparse

LANG_NAMES = {
    "eng":"English","jpn":"Japanese","ara":"Arabic","fre":"French","fra":"French",
    "ger":"German","deu":"German","ita":"Italian","por":"Portuguese","spa":"Spanish",
    "rus":"Russian","pol":"Polish","dut":"Dutch","nld":"Dutch","tur":"Turkish",
    "kor":"Korean","chi":"Chinese","zho":"Chinese","vie":"Vietnamese","ind":"Indonesian",
    "tha":"Thai","heb":"Hebrew","ron":"Romanian","rum":"Romanian","ukr":"Ukrainian",
    "hun":"Hungarian","ces":"Czech","cze":"Czech","gre":"Greek","ell":"Greek",
    "fin":"Finnish","swe":"Swedish","nor":"Norwegian","dan":"Danish","und":"Undetermined",
}
# text subtitle codecs convertible to WebVTT; everything else is bitmap (needs OCR)
TEXT_SUBS = {"ass","ssa","subrip","srt","webvtt","mov_text","text"}

def lang_name(code, title):
    if title:
        return title
    return LANG_NAMES.get((code or "und").lower(), (code or "und"))

def run(cmd, allow_fail=False):
    t0 = time.time()
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    dt = time.time() - t0
    if p.returncode != 0 and not allow_fail:
        sys.stderr.write(f"\n[FAIL] {' '.join(cmd)}\n{p.stdout[-2000:]}\n")
        raise SystemExit(1)
    return dt, p.stdout, p.returncode

def probe(path):
    out = subprocess.check_output([
        "ffprobe", "-v", "error", "-show_entries",
        "format=duration:stream=index,codec_type,codec_name,pix_fmt,width,height,channels:stream_tags=language,title",
        "-of", "json", path], text=True)
    return json.loads(out)

def dir_bytes(d):
    return sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(d) for f in fs)

def seg_stats(d):
    segs = [os.path.join(d, f) for f in os.listdir(d) if f.endswith(".ts")]
    if not segs:
        return {"count": 0, "avg_kb": 0, "max_kb": 0}
    sizes = [os.path.getsize(s) for s in segs]
    tot = sum(sizes)
    return {"count": len(segs), "avg_kb": round(tot / len(segs) / 1024, 1),
            "max_kb": round(max(sizes) / 1024, 1)}

HLS_TIME = 6
HLS_COMMON = ["-hls_time", str(HLS_TIME), "-hls_playlist_type", "vod",
              "-hls_flags", "independent_segments", "-hls_segment_type", "mpegts"]

# Anime-tuned CONSTANT-QUALITY ladder. cq (NVENC) / crf (libx264) drive perceptual
# quality, NOT a fixed bitrate; maxrate is only a safety ceiling so a grainy or
# high-motion scene can't blow the file up. Anime compresses extremely well, so
# typical output lands far below the ceiling (~2-3 Mbps @1080p). The native
# (source-resolution) rendition is now ALWAYS re-encoded to this target — we no
# longer remux fat WEB-DL/BD sources straight through, so every cached 1080p is a
# consistent ~3 Mbps. Tune with --cq (native rendition) or per-height here.
QUALITY = {
    1080: {"cq": 24, "crf": 21, "maxrate": "5000k"},
    720:  {"cq": 24, "crf": 21, "maxrate": "2800k"},
    480:  {"cq": 25, "crf": 22, "maxrate": "1400k"},
}
DEFAULT_Q = {"cq": 24, "crf": 21, "maxrate": "5000k"}

def _double_rate(r):
    """'5000k' -> '10000k' (2x maxrate is a sane VBV bufsize)."""
    try:
        return f"{int(str(r).rstrip('k')) * 2}k"
    except ValueError:
        return r

def build_video_rendition(src, outdir, height, native, src_is_h264_8bit, use_nvenc, q, allow_remux=False):
    """Build one HLS video rendition. The native (source-resolution) rendition and the
    downscaled rungs are ALL re-encoded to the constant-quality target in `q`
    ({cq,crf,maxrate}). Only re-muxes losslessly when allow_remux + H.264/8-bit source
    (opt-in escape hatch; OFF by default so fat WEB-DL/BD sources get normalised)."""
    os.makedirs(outdir, exist_ok=True)
    seg = os.path.join(outdir, "seg%03d.ts")
    idx = os.path.join(outdir, "index.m3u8")
    if native and allow_remux and src_is_h264_8bit:
        cmd = ["ffmpeg", "-y", "-i", src, "-map", "0:v:0", "-c:v", "copy", "-an", "-sn",
               *HLS_COMMON, "-hls_segment_filename", seg, idx]
        mode = "remux"
    else:
        maxr = q["maxrate"]; buf = maxr   # NVENC VBV: bufsize==maxrate (2x gave no tighter cap)
        vf = "scale=-2:trunc(ih/2)*2" if native else f"scale=-2:{height}"
        if use_nvenc:
            cq = q["cq"]
            enc = ["h264_nvenc", "-preset", "p5", "-rc", "vbr", "-cq", str(cq),
                   "-b:v", "0", "-maxrate", maxr, "-bufsize", buf,
                   "-spatial-aq", "1", "-temporal-aq", "1", "-rc-lookahead", "20",
                   "-pix_fmt", "yuv420p", "-profile:v", "high"]
            mode = f"nvenc cq{cq}"
        else:
            crf = q["crf"]
            enc = ["libx264", "-preset", "slow", "-crf", str(crf),
                   "-maxrate", maxr, "-bufsize", buf, "-tune", "animation",
                   "-pix_fmt", "yuv420p", "-profile:v", "high"]
            mode = f"x264 crf{crf}"
        cmd = ["ffmpeg", "-y", "-i", src, "-map", "0:v:0", "-vf", vf, "-c:v", *enc, "-an", "-sn",
               *HLS_COMMON, "-hls_segment_filename", seg, idx]
    dt, _, _ = run(cmd)
    return {"mode": mode, "seconds": round(dt, 2), "bytes": dir_bytes(outdir), **seg_stats(outdir)}

def build_audio_rendition(src, outdir, a_index):
    os.makedirs(outdir, exist_ok=True)
    seg = os.path.join(outdir, "seg%03d.ts")
    idx = os.path.join(outdir, "index.m3u8")
    cmd = ["ffmpeg", "-y", "-i", src, "-map", f"0:a:{a_index}", "-c:a", "aac", "-b:a", "128k", "-vn", "-sn",
           *HLS_COMMON, "-hls_segment_filename", seg, idx]
    dt, _, _ = run(cmd)
    return {"seconds": round(dt, 2), "bytes": dir_bytes(outdir), **seg_stats(outdir)}

def extract_sub(src, subs_dir, s_index, lang, title, codec, duration):
    os.makedirs(subs_dir, exist_ok=True)
    base = f"{lang}{s_index}"
    name = lang_name(lang, title)
    if codec not in TEXT_SUBS:
        # bitmap subtitle (PGS/VOBSUB/DVB) — cannot convert to WebVTT; keep original
        run(["ffmpeg", "-y", "-i", src, "-map", f"0:s:{s_index}", "-c:s", "copy",
             os.path.join(subs_dir, f"{base}.{codec}")], allow_fail=True)
        return {"lang": lang, "name": name, "codec": codec, "converted": False,
                "note": "bitmap subtitle — not converted to WebVTT (needs OCR)"}
    vtt = os.path.join(subs_dir, base + ".vtt")
    dt, _, _ = run(["ffmpeg", "-y", "-i", src, "-map", f"0:s:{s_index}", "-c:s", "webvtt", vtt])
    # keep original format too (e.g. ASS for faithful JASSUB/SubtitlesOctopus render)
    orig = "ass" if codec in ("ass", "ssa") else ("srt" if codec in ("subrip", "srt") else codec)
    run(["ffmpeg", "-y", "-i", src, "-map", f"0:s:{s_index}", "-c:s", "copy",
         os.path.join(subs_dir, f"{base}.{orig}")], allow_fail=True)
    m3u8 = os.path.join(subs_dir, base + ".m3u8")
    with open(m3u8, "w") as f:
        f.write("#EXTM3U\n#EXT-X-VERSION:6\n#EXT-X-TARGETDURATION:%d\n"
                "#EXT-X-PLAYLIST-TYPE:VOD\n#EXTINF:%.3f,\n%s\n#EXT-X-ENDLIST\n"
                % (int(duration) + 1, duration, base + ".vtt"))
    return {"lang": lang, "name": name, "codec": codec, "converted": True,
            "seconds": round(dt, 2), "uri": f"subs/{base}.m3u8",
            "vtt": f"subs/{base}.vtt",
            "ass": f"subs/{base}.ass" if orig in ("ass", "ssa") else None,
            "bytes": os.path.getsize(vtt)}

def extract_fonts(src, fonts_dir):
    """Dump embedded font attachments (TTF/OTF) into fonts_dir — JASSUB needs them
    to render ASS faithfully (signs/styles reference these fonts by name)."""
    os.makedirs(fonts_dir, exist_ok=True)
    # -dump_attachment writes each attachment to its filename in the CWD; run there.
    subprocess.run(["ffmpeg", "-y", "-dump_attachment:t", "", "-i", src],
                   cwd=fonts_dir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return sorted(f for f in os.listdir(fonts_dir)
                  if f.lower().endswith((".ttf", ".otf", ".ttc", ".eot", ".woff", ".woff2")))

def write_master(out, video_rends, audio_rends, sub_rends):
    lines = ["#EXTM3U", "#EXT-X-VERSION:6"]
    # Default audio = Japanese (the "sub" experience) when present, else the first
    # track. The player's audio selector + the SUB/DUB control can switch to a dub.
    def_aud = next((i for i, a in enumerate(audio_rends) if (a["lang"] or "").startswith("ja")),
                   0 if audio_rends else None)
    for i, a in enumerate(audio_rends):
        lines.append('#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="aud",NAME="%s",LANGUAGE="%s",'
                     'DEFAULT=%s,AUTOSELECT=YES,URI="%s"' %
                     (a["name"], a["lang"], "YES" if i == def_aud else "NO", a["uri"]))
    # default subtitle = first English track, else first
    def_idx = next((i for i, s in enumerate(sub_rends) if s["lang"].startswith("en")),
                   0 if sub_rends else None)
    for i, s in enumerate(sub_rends):
        lines.append('#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="%s",LANGUAGE="%s",'
                     'DEFAULT=%s,AUTOSELECT=YES,FORCED=NO,URI="%s"' %
                     (s["name"], s["lang"], "YES" if i == def_idx else "NO", s["uri"]))
    audio_attr = ',AUDIO="aud"' if audio_rends else ""
    subs_attr = ',SUBTITLES="subs"' if sub_rends else ""
    for v in video_rends:
        lines.append('#EXT-X-STREAM-INF:BANDWIDTH=%d,RESOLUTION=%s,CODECS="avc1.640028,mp4a.40.2"%s%s'
                     % (v["bandwidth"], v["resolution"], audio_attr, subs_attr))
        lines.append(v["uri"])
    path = os.path.join(out, "master.m3u8")
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")
    return path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("src")
    ap.add_argument("out")
    ap.add_argument("--renditions", default="1080,720,480")
    ap.add_argument("--no-nvenc", action="store_true")
    ap.add_argument("--cq", type=int, default=None,
                    help="override NVENC CQ for the native rendition "
                         "(default %d; lower = better quality / bigger)" % QUALITY[1080]["cq"])
    ap.add_argument("--remux-native", action="store_true",
                    help="losslessly copy the native rendition when source is H.264/8-bit "
                         "instead of re-encoding (keeps the source's bitrate; OFF by default)")
    args = ap.parse_args()

    if os.path.exists(args.out):
        shutil.rmtree(args.out)
    os.makedirs(args.out)

    info = probe(args.src)
    duration = float(info["format"]["duration"])
    vstreams = [s for s in info["streams"] if s["codec_type"] == "video"]
    astreams = [s for s in info["streams"] if s["codec_type"] == "audio"]
    sstreams = [s for s in info["streams"] if s["codec_type"] == "subtitle"]
    v0 = vstreams[0]
    src_h = v0.get("height", 1080)
    src_is_h264_8bit = (v0["codec_name"] == "h264" and "p10" not in (v0.get("pix_fmt") or ""))
    use_nvenc = not args.no_nvenc

    report = {"src": args.src, "duration_s": round(duration, 1),
              "src_video": {"codec": v0["codec_name"], "pix_fmt": v0.get("pix_fmt"),
                            "height": src_h, "h264_8bit": src_is_h264_8bit},
              "src_audio_tracks": len(astreams), "src_sub_tracks": len(sstreams),
              "nvenc": use_nvenc, "video": [], "audio": [], "subs": []}

    wanted = [int(x) for x in args.renditions.split(",")]
    ladder = [("v0", src_h, True)]
    vi = 1
    for h in wanted:
        if h < src_h:
            ladder.append((f"v{vi}", h, False)); vi += 1

    video_rends_meta = []
    for name, h, native in ladder:
        q = dict(QUALITY.get(h, DEFAULT_Q))
        if native and args.cq is not None:
            q["cq"] = args.cq
        m = build_video_rendition(args.src, os.path.join(args.out, name), h,
                                  native, src_is_h264_8bit, use_nvenc, q,
                                  allow_remux=args.remux_native)
        m["realtime_x"] = round(duration / m["seconds"], 1) if m["seconds"] > 0 else None
        m["mbps"] = round(m["bytes"] * 8 / duration / 1e6, 2) if duration > 0 else None
        m.update({"name": name, "height": h, "native": native})
        report["video"].append(m)
        # Declare BANDWIDTH from the ACTUAL encode. NVENC's maxrate is soft, so a few
        # complex segments overshoot ~50%; using the single peak segment would over-
        # declare (one 8 MB keyframe seg → 11 Mbps) and make ABR avoid this rung on
        # capable links. avg×1.5 gives realistic burst headroom (the player's segment
        # buffer absorbs brief spikes as long as the average fits the link).
        avg_bps = int(m["bytes"] * 8 / duration) if duration > 0 else 0
        bandwidth = max(int(avg_bps * 1.5), 200000)
        w = int(round(h * 16 / 9 / 2) * 2)
        video_rends_meta.append({"bandwidth": bandwidth, "resolution": f"{w}x{h}", "uri": f"{name}/index.m3u8"})

    audio_rends_meta = []
    for i, a in enumerate(astreams):
        m = build_audio_rendition(args.src, os.path.join(args.out, f"a{i}"), i)
        tags = a.get("tags", {}) or {}
        lang = tags.get("language", f"a{i}")
        nm = lang_name(lang, tags.get("title"))
        m.update({"name": nm, "lang": lang})
        report["audio"].append(m)
        audio_rends_meta.append({"name": nm, "lang": lang, "uri": f"a{i}/index.m3u8"})

    sub_rends_meta = []
    sub_tracks = []   # for subs/tracks.json (ASS + VTT per language, for JASSUB)
    used = {}
    for i, s in enumerate(sstreams):
        tags = s.get("tags", {}) or {}
        lang = tags.get("language", f"s{i}")
        codec = s.get("codec_name", "")
        try:
            m = extract_sub(args.src, os.path.join(args.out, "subs"), i, lang,
                            tags.get("title"), codec, duration)
        except SystemExit:
            report["subs"].append({"lang": lang, "error": "extract failed"}); continue
        report["subs"].append(m)
        if m.get("converted"):
            cnt = used.get(m["name"], 0) + 1; used[m["name"]] = cnt
            disp = m["name"] if cnt == 1 else f"{m['name']} ({cnt})"
            sub_rends_meta.append({"lang": lang, "name": disp, "uri": m["uri"]})
            sub_tracks.append({"lang": lang, "name": disp, "vtt": m.get("vtt"),
                               "ass": m.get("ass"), "default": False})

    # font attachments + a manifest the player uses to render ASS faithfully (JASSUB)
    fonts = extract_fonts(args.src, os.path.join(args.out, "subs", "fonts"))
    if sub_tracks:
        di = next((j for j, t in enumerate(sub_tracks) if t["lang"].startswith("en")), 0)
        sub_tracks[di]["default"] = True
    os.makedirs(os.path.join(args.out, "subs"), exist_ok=True)
    with open(os.path.join(args.out, "subs", "tracks.json"), "w") as f:
        json.dump({"subs": sub_tracks, "fonts": [f"subs/fonts/{x}" for x in fonts]}, f)
    report["fonts"] = len(fonts)

    write_master(args.out, video_rends_meta, audio_rends_meta, sub_rends_meta)

    total = dir_bytes(args.out)
    report["total_bytes"] = total
    report["total_mb"] = round(total / 1024 / 1024, 1)
    report["subs_converted"] = sum(1 for s in report["subs"] if s.get("converted"))
    report["subs_bitmap_skipped"] = sum(1 for s in report["subs"] if s.get("converted") is False)
    if duration > 0:
        report["extrapolated_24min_gb"] = round(total / 1024 / 1024 / 1024 * (1440 / duration), 3)
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
