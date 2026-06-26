import json, urllib.request, re
def hj(url):
    req=urllib.request.Request(url, headers={"Accept":"application/json","User-Agent":"anichan-test"})
    with urllib.request.urlopen(req, timeout=25) as r: return json.load(r)
at=hj("https://feed.animetosho.org/json?aid=19381&only_tor=1")
def res(t):
    m=re.search(r"(1080|720|480)p", t); return m.group(1) if m else None
def multisub(t): return bool(re.search(r"multi.?sub", t, re.I))
def hevc(t): return bool(re.search(r"hevc|x265|av1", t, re.I))
def size(e): return int(e.get("total_size") or 0)
def seed(e): return int(e.get("seeders") or 0)
# one episode with lots of releases
from collections import Counter
eps=Counter(e.get("anidb_eid") for e in at if e.get("anidb_eid"))
target=eps.most_common(1)[0][0]
rels=[e for e in at if e.get("anidb_eid")==target]
# master = 1080p, multisub, H.264 (not hevc/av1), prefer Erai-raws, big, well-seeded
cand_m=[e for e in rels if res(e["title"])=="1080" and multisub(e["title"]) and not hevc(e["title"]) and size(e)>900*1048576]
master=max(cand_m, key=lambda e:(("Erai-raws" in e["title"]), seed(e)))
# hevc = 1080p multisub hevc
cand_h=[e for e in rels if res(e["title"])=="1080" and multisub(e["title"]) and hevc(e["title"])]
hv=max(cand_h, key=seed) if cand_h else None
print("MASTER (1080p H.264 MultiSub):", master["title"])
print("   torrent_url:", master["torrent_url"])
print("HEVC (1080p MultiSub):", hv["title"] if hv else "none")
if hv: print("   torrent_url:", hv["torrent_url"])
open("/tmp/master_url","w").write(master["torrent_url"])
if hv: open("/tmp/hevc_url","w").write(hv["torrent_url"])
