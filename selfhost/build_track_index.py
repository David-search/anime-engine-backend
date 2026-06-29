#!/usr/bin/env python3
"""build_track_index.py — join the AnimeTosho `attachments` dump (per-file subtitle tracks:
lang+codec) with the `files` dump (file -> torrent) to record, per torrent, which SUBTITLE
languages it actually carries. Lets dump_resolver pick releases by REAL track metadata
(has English sub? how many languages?) instead of guessing from the release name.

Writes table `tsubs(torrent_id, has_eng, n_langs)` into at_index.sqlite.
"""
import lzma, json, sqlite3, os, time, sys

DB = os.getenv("AT_INDEX", "/data/at_index.sqlite")
DUMP = "/data/at_dump"
t0 = time.time()

def unescape(s):  # reverse the dump's C-style TSV escaping enough to parse the JSON
    return s.replace("\\t", "\t").replace("\\n", "\n").replace("\\\\", "\\")

# ---- pass 1: file_id -> set(subtitle langs) ----
file_langs = {}
f = lzma.open(f"{DUMP}/attachments-latest.txt.xz", "rt", encoding="utf-8", errors="ignore"); f.readline()
for line in f:
    r = line.rstrip("\n").split("\t")
    if len(r) < 2: continue
    try:
        arr = json.loads(unescape(r[1]))
    except Exception:
        continue
    subs = arr[1] if isinstance(arr, list) and len(arr) > 1 and isinstance(arr[1], list) else None
    if subs:
        langs = {s.get("lang") for s in subs if isinstance(s, dict) and s.get("lang")}
        if langs:
            try: file_langs[int(r[0])] = langs
            except ValueError: pass
f.close()
print(f"pass1: {len(file_langs):,} files with subtitle tracks ({time.time()-t0:.0f}s)", flush=True)

# ---- pass 2: file -> torrent, union langs per torrent ----
f = lzma.open(f"{DUMP}/files-latest.txt.xz", "rt", encoding="utf-8", errors="ignore")
hdr = f.readline().rstrip("\n").split("\t"); ix = {c: i for i, c in enumerate(hdr)}
fi, ti = ix.get("id"), ix.get("torrent_id")
tsubs = {}
for line in f:
    r = line.rstrip("\n").split("\t")
    if fi is None or ti is None or len(r) <= max(fi, ti): continue
    try: fid, tid = int(r[fi]), int(r[ti])
    except ValueError: continue
    lg = file_langs.get(fid)
    if lg: tsubs.setdefault(tid, set()).update(lg)
f.close()
print(f"pass2: {len(tsubs):,} torrents carry subtitles ({time.time()-t0:.0f}s)", flush=True)

# ---- write ----
con = sqlite3.connect(DB); cur = con.cursor()
cur.execute("DROP TABLE IF EXISTS tsubs")
cur.execute("CREATE TABLE tsubs(torrent_id INTEGER PRIMARY KEY, has_eng INTEGER, n_langs INTEGER)")
con.execute("BEGIN")
cur.executemany("INSERT OR REPLACE INTO tsubs VALUES(?,?,?)",
                ((tid, 1 if "eng" in lg else 0, len(lg)) for tid, lg in tsubs.items()))
con.commit()
tot = con.execute("SELECT COUNT(*) FROM tsubs").fetchone()[0]
eng = con.execute("SELECT COUNT(*) FROM tsubs WHERE has_eng=1").fetchone()[0]
print(f"DONE: tsubs={tot:,} | has_eng={eng:,} ({100*eng//max(tot,1)}%) | {time.time()-t0:.0f}s", flush=True)
