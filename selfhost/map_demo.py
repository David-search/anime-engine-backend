import json, urllib.request, re

def hj(url, data=None):
    req = urllib.request.Request(url, data=data,
        headers={"Content-Type":"application/json","Accept":"application/json","User-Agent":"anichan-test"})
    with urllib.request.urlopen(req, timeout=25) as r:
        return json.load(r)

# 1) AniList: title -> id
q="query($s:String){Media(search:$s,type:ANIME){id idMal episodes title{romaji english}}}"
al=hj("https://graphql.anilist.co", json.dumps({"query":q,"variables":{"s":"The Beginning After the End Season 2"}}).encode())
m=al["data"]["Media"]; anilist_id=m["id"]
title=m["title"]["english"] or m["title"]["romaji"]
print("1) AniList id:", anilist_id, "|", title, "| eps:", m["episodes"])

# 2) ani.zip: anilist_id -> anidb_id  (mapping Nyaa cannot give)
az=hj("https://api.ani.zip/mappings?anilist_id=%d" % anilist_id)
anidb=az.get("mappings",{}).get("anidb_id")
print("2) AniDB id (via ani.zip):", anidb)

# 3) AnimeTosho structured releases for this anime
at=hj("https://feed.animetosho.org/json?aid=%s&only_tor=1" % anidb)
print("3) AnimeTosho returned %d releases for AniDB aid=%s" % (len(at), anidb))

# group by episode, show quality + sub variety
def res_of(t):
    mm=re.search(r"(2160|1080|720|540|480)p", t); return (mm.group(0) if mm else "?")
def multisub(t):
    return "MULTI" if re.search(r"multi.?sub", t, re.I) else ("sub" )
by_ep={}
for e in at:
    eid=e.get("anidb_eid")
    if eid: by_ep.setdefault(eid, []).append(e)
print("   episodes with releases:", len(by_ep))
# pick the episode with the most releases to show variety
ep, rels = max(by_ep.items(), key=lambda kv: len(kv[1]))
print("   --- ep anidb_eid=%s : %d releases (qualities + subs offered) ---" % (ep, len(rels)))
seen=set()
for e in sorted(rels, key=lambda x: -int(x.get("seeders") or 0)):
    t=e.get("title",""); key=(res_of(t), multisub(t), t.split("]")[0])
    line="   %-6s | %-5s | %4s seed | %5s MB | tor=%s nzb=%s | %s" % (
        res_of(t), multisub(t), e.get("seeders"),
        round(int(e.get("total_size",0))/1048576), "Y" if e.get("torrent_url") else "-",
        "Y" if e.get("nzb_url") else "-", t[:64])
    print(line)
qualities=sorted({res_of(e.get("title","")) for e in rels})
print("   => distinct qualities available for this ep:", qualities)
