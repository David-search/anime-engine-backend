#!/usr/bin/env python3
"""
cache_db.py — SQLite cache index + LRU eviction for the HLS-at-rest cache.

One row per cached (anilist_id, ep, category) HLS package under
/data/cache/{anilist_id}/{ep}/{category}/. The ingester registers on build; the
evictor deletes least-recently-accessed *whole episodes* when the cache exceeds
a GB cap (pinned rows are never evicted). The serving layer should call touch()
so last_access reflects real viewing.

CLI:  cache_db.py stats | list | reindex | evict <cap_gb> | pin <aid> <ep> | touch <aid> <ep>
"""
import sqlite3, os, time, json, shutil, sys, argparse, glob, re

DB_DEFAULT = "/data/cache/index.db"
CACHE_ROOT = "/data/cache"

SCHEMA = """
CREATE TABLE IF NOT EXISTS episodes (
  anilist_id   INTEGER NOT NULL,
  ep           INTEGER NOT NULL,
  category     TEXT NOT NULL DEFAULT 'sub',
  path         TEXT NOT NULL,
  bytes        INTEGER NOT NULL,
  created      REAL NOT NULL,
  last_access  REAL NOT NULL,
  renditions   TEXT,
  audio_tracks INTEGER,
  sub_langs    TEXT,
  source_title TEXT,
  pinned       INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (anilist_id, ep, category)
);
CREATE TABLE IF NOT EXISTS mapping_cache (
  anilist_id INTEGER PRIMARY KEY,
  payload    TEXT NOT NULL,
  fetched    REAL NOT NULL
);
"""

def conn(db=DB_DEFAULT):
    os.makedirs(os.path.dirname(db), exist_ok=True)
    c = sqlite3.connect(db, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.executescript(SCHEMA)
    return c

def dir_bytes(d):
    tot = 0
    for r, _, fs in os.walk(d):
        for f in fs:
            p = os.path.join(r, f)
            if os.path.exists(p):
                tot += os.path.getsize(p)
    return tot

def register(anilist_id, ep, category, path, renditions=None, audio_tracks=None,
             sub_langs=None, source_title=None, pinned=0, db=DB_DEFAULT):
    b = dir_bytes(path)
    t = time.time()
    c = conn(db)
    c.execute("""INSERT INTO episodes
        (anilist_id,ep,category,path,bytes,created,last_access,renditions,audio_tracks,sub_langs,source_title,pinned)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(anilist_id,ep,category) DO UPDATE SET
          path=excluded.path, bytes=excluded.bytes, last_access=excluded.last_access,
          renditions=excluded.renditions, audio_tracks=excluded.audio_tracks,
          sub_langs=excluded.sub_langs, source_title=excluded.source_title""",
        (anilist_id, ep, category, path, b, t, t,
         json.dumps(renditions or []), audio_tracks, json.dumps(sub_langs or []),
         source_title, pinned))
    c.commit(); c.close()
    return b

def touch(anilist_id, ep, category="sub", db=DB_DEFAULT):
    c = conn(db)
    c.execute("UPDATE episodes SET last_access=? WHERE anilist_id=? AND ep=? AND category=?",
              (time.time(), anilist_id, ep, category))
    c.commit(); c.close()

def set_pin(anilist_id, ep, category="sub", pinned=1, db=DB_DEFAULT):
    c = conn(db)
    c.execute("UPDATE episodes SET pinned=? WHERE anilist_id=? AND ep=? AND category=?",
              (pinned, anilist_id, ep, category))
    c.commit(); c.close()

def is_cached(anilist_id, ep, category="sub", db=DB_DEFAULT):
    c = conn(db)
    r = c.execute("SELECT path FROM episodes WHERE anilist_id=? AND ep=? AND category=?",
                  (anilist_id, ep, category)).fetchone()
    c.close()
    return bool(r and r[0] and os.path.isdir(r[0]))

def cached_eps(anilist_id, category="sub", db=DB_DEFAULT):
    """Sorted list of episode numbers cached for this (anime, category)."""
    c = conn(db)
    rows = c.execute("SELECT ep FROM episodes WHERE anilist_id=? AND category=? ORDER BY ep",
                     (anilist_id, category)).fetchall()
    c.close()
    return [r[0] for r in rows]

def mapping_get(anilist_id, db=DB_DEFAULT):
    """Cached ani.zip→AniDB mapping payload for an anime: (payload_json, fetched_epoch) or None."""
    c = conn(db)
    r = c.execute("SELECT payload, fetched FROM mapping_cache WHERE anilist_id=?", (anilist_id,)).fetchone()
    c.close()
    return (r[0], r[1]) if r else None

def mapping_put(anilist_id, payload, db=DB_DEFAULT):
    """Persist a freshly-computed mapping so re-ingests don't re-hit ani.zip (and
    survive an ani.zip outage). Caller serializes the payload to JSON."""
    c = conn(db)
    c.execute("INSERT INTO mapping_cache(anilist_id,payload,fetched) VALUES(?,?,?) "
              "ON CONFLICT(anilist_id) DO UPDATE SET payload=excluded.payload, fetched=excluded.fetched",
              (anilist_id, payload, time.time()))
    c.commit(); c.close()

def total_bytes(db=DB_DEFAULT):
    c = conn(db)
    r = c.execute("SELECT COALESCE(SUM(bytes),0) FROM episodes").fetchone()[0]
    c.close(); return r

def stats(db=DB_DEFAULT):
    c = conn(db)
    n, b, p = c.execute("SELECT COUNT(*),COALESCE(SUM(bytes),0),COALESCE(SUM(pinned),0) FROM episodes").fetchone()
    rows = c.execute("""SELECT anilist_id,ep,category,bytes,last_access,sub_langs,renditions,pinned,source_title
                        FROM episodes ORDER BY last_access""").fetchall()
    c.close()
    return {"count": n, "total_gb": round(b/1024**3, 3), "pinned": p, "rows": rows}

def evict(cap_gb, db=DB_DEFAULT):
    cap = cap_gb * 1024**3
    c = conn(db)
    freed, removed = 0, []
    while True:
        tot = c.execute("SELECT COALESCE(SUM(bytes),0) FROM episodes").fetchone()[0]
        if tot <= cap:
            break
        row = c.execute("""SELECT anilist_id,ep,category,path,bytes FROM episodes
                           WHERE pinned=0 ORDER BY last_access LIMIT 1""").fetchone()
        if not row:
            break  # only pinned rows remain; cannot go lower
        aid, ep, cat, path, b = row
        if path and os.path.isdir(path):
            shutil.rmtree(path, ignore_errors=True)
            # remove now-empty {ep}/ and {anilist_id}/ parents
            for parent in (os.path.dirname(path), os.path.dirname(os.path.dirname(path))):
                try:
                    if parent and os.path.isdir(parent) and not os.listdir(parent):
                        os.rmdir(parent)
                except OSError:
                    pass
        c.execute("DELETE FROM episodes WHERE anilist_id=? AND ep=? AND category=?", (aid, ep, cat))
        c.commit()
        freed += b; removed.append({"anilist_id": aid, "ep": ep, "category": cat, "gb": round(b/1024**3, 3)})
    c.close()
    return {"freed_gb": round(freed/1024**3, 3), "removed": removed, "evicted": len(removed)}

def reindex(root=CACHE_ROOT, db=DB_DEFAULT):
    """Bootstrap the index by scanning {root}/{anilist_id}/{ep}/{cat}/master.m3u8."""
    found = []
    for master in glob.glob(os.path.join(root, "*", "*", "*", "master.m3u8")):
        d = os.path.dirname(master)
        parts = d.rstrip("/").split("/")
        cat, ep, aid = parts[-1], parts[-2], parts[-3]
        try:
            aid_i, ep_i = int(aid), int(ep)
        except ValueError:
            continue  # skip non-numeric demo dirs (test1/, real/, erai/)
        txt = open(master).read()
        rends = re.findall(r"RESOLUTION=\d+x(\d+)", txt)
        subs = re.findall(r'TYPE=SUBTITLES,GROUP-ID="subs",NAME="[^"]*",LANGUAGE="([^"]+)"', txt)
        auds = len(re.findall(r"TYPE=AUDIO", txt))
        register(aid_i, ep_i, cat, d, renditions=[f"{r}p" for r in rends],
                 audio_tracks=auds, sub_langs=subs, db=db)
        found.append((aid_i, ep_i, cat))
    return found

def _print_stats(db):
    s = stats(db)
    print(f"cache: {s['count']} episodes, {s['total_gb']} GB, {s['pinned']} pinned")
    for r in s["rows"]:
        aid, ep, cat, b, la, subs, rends, pin, title = r
        age = round((time.time() - la) / 60, 1)
        print(f"  [{'P' if pin else ' '}] {aid} ep{ep:>3} {cat:4} | {round(b/1024**2):>5} MB | "
              f"{','.join(json.loads(rends or '[]')):<14} | subs {len(json.loads(subs or '[]'))} | "
              f"last_access {age}m ago")

def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("stats"); sub.add_parser("list"); sub.add_parser("reindex")
    e = sub.add_parser("evict"); e.add_argument("cap_gb", type=float)
    pn = sub.add_parser("pin"); pn.add_argument("aid", type=int); pn.add_argument("ep", type=int)
    to = sub.add_parser("touch"); to.add_argument("aid", type=int); to.add_argument("ep", type=int)
    ap.add_argument("--db", default=DB_DEFAULT)
    a = ap.parse_args()
    if a.cmd in ("stats", "list"):
        _print_stats(a.db)
    elif a.cmd == "reindex":
        f = reindex(db=a.db); print(f"reindexed {len(f)} packages"); _print_stats(a.db)
    elif a.cmd == "evict":
        print(json.dumps(evict(a.cap_gb, db=a.db), indent=2)); _print_stats(a.db)
    elif a.cmd == "pin":
        set_pin(a.aid, a.ep, db=a.db); print(f"pinned {a.aid} ep{a.ep}")
    elif a.cmd == "touch":
        touch(a.aid, a.ep, db=a.db); print(f"touched {a.aid} ep{a.ep}")

if __name__ == "__main__":
    main()
