#!/usr/bin/env python3
"""build_index.py — build the local SQLite RESOLVE index from the AnimeTosho `torrents-latest`
dump, so AniList->aid/eid resolution is a local query (no live feed, no 429). Run once after
downloading the dump (and after build_track_index.py for subtitle metadata). Rebuild whenever
the dump is refreshed.

  torrents(id, aid, eid, fid, name, magnet, btih, nyaa_id, totalsize, stored_nzb, labels, gids,
           deleted, status)   indexed on (aid,eid) and (aid)
"""
import lzma, sqlite3, os, time

DUMP = os.getenv("AT_TORRENTS", "/data/at_dump/torrents-latest.txt.xz")
DB = os.getenv("AT_INDEX", "/data/at_index.sqlite")

def main():
    t0 = time.time()
    if os.path.exists(DB):
        os.remove(DB)
    con = sqlite3.connect(DB); cur = con.cursor()
    cur.execute("""CREATE TABLE torrents(id INT,aid INT,eid INT,fid INT,name TEXT,magnet TEXT,
      btih TEXT,nyaa_id INT,totalsize INT,stored_nzb INT,labels INT,gids TEXT,deleted INT,status INT)""")
    f = lzma.open(DUMP, "rt", encoding="utf-8", errors="ignore")
    hdr = f.readline().rstrip("\n").split("\t"); ix = {c: i for i, c in enumerate(hdr)}
    def S(r, k, d=""):
        j = ix.get(k); return r[j] if j is not None and j < len(r) else d
    def N(r, k):
        try: return int(S(r, k, "0") or 0)
        except ValueError: return 0
    batch = []; total = 0
    con.execute("BEGIN")
    for line in f:
        r = line.rstrip("\n").split("\t")
        if len(r) < len(hdr): continue
        aid = N(r, "aid")
        if aid == 0:                          # only AniDB-mapped torrents are resolvable
            continue
        batch.append((N(r, "id"), aid, N(r, "eid"), N(r, "fid"), S(r, "name"), S(r, "magnet"),
                      S(r, "btih"), N(r, "nyaa_id"), N(r, "totalsize"), N(r, "stored_nzb"),
                      N(r, "anidex_labels"), S(r, "gids"), N(r, "deleted"), N(r, "status")))
        if len(batch) >= 5000:
            cur.executemany("INSERT INTO torrents VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", batch)
            total += len(batch); batch = []
    if batch:
        cur.executemany("INSERT INTO torrents VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", batch); total += len(batch)
    con.commit(); f.close()
    cur.execute("CREATE INDEX i_aid_eid ON torrents(aid,eid)")
    cur.execute("CREATE INDEX i_aid ON torrents(aid)")
    con.commit()
    print(f"BUILT {DB}: {total:,} rows ({os.path.getsize(DB)/1e6:.0f} MB) in {time.time()-t0:.0f}s")
    print(f"  with NZB: {cur.execute('SELECT COUNT(*) FROM torrents WHERE stored_nzb=1').fetchone()[0]:,}")

if __name__ == "__main__":
    main()
