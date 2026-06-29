#!/usr/bin/env python3
"""completeness_test.py — measure how many of our AnimeTosho NZBs are actually
RETRIEVABLE on the configured Usenet provider (e.g. Eweka), WITHOUT downloading.

For a random sample of stored_nzb=1 entries from the dump index, fetch the small NZB
XML, then NNTP `STAT` a few of its article message-ids against the provider (223=present,
430=missing). An NZB counts as FULL if all sampled articles are present. Tells us whether
one backbone suffices or we need a second-backbone block account for backfill.

Creds are read from /data/nzbget.conf (Server1.*) — not passed on the command line.
Env: SAMPLE (NZBs, default 150), PER_NZB (articles per NZB, default 4).
"""
import ssl, socket, random, urllib.request, urllib.parse, sqlite3, re, os, time

CONF = os.getenv("NZBGET_CONF", "/data/nzbget.conf")
DB = os.getenv("AT_INDEX", "/data/at_index.sqlite")
SAMPLE = int(os.getenv("SAMPLE", "150"))
PER = int(os.getenv("PER_NZB", "4"))

def cfg(key, d=""):
    try:
        for l in open(CONF):
            if l.startswith(key + "="):
                return l.split("=", 1)[1].strip()
    except OSError:
        pass
    return d

HOST = cfg("Server1.Host", "news.eweka.nl"); PORT = int(cfg("Server1.Port", "563"))
USER = cfg("Server1.Username"); PASS = cfg("Server1.Password")

def nzb_url(tid, name):
    return f"https://storage.animetosho.org/nzbs/{tid:08x}/{urllib.parse.quote(name)}.nzb"

class NNTP:
    def __init__(self):
        self.connect()
    def connect(self):
        raw = socket.create_connection((HOST, PORT), timeout=30)
        self.sock = ssl.create_default_context().wrap_socket(raw, server_hostname=HOST)
        self.f = self.sock.makefile("rb")
        self._read()                                   # greeting
        self._cmd(f"AUTHINFO USER {USER}")
        self._cmd(f"AUTHINFO PASS {PASS}")
    def _read(self):
        return self.f.readline().decode("latin1", errors="ignore").strip()
    def _cmd(self, c):
        self.sock.sendall((c + "\r\n").encode("latin1", "ignore")); return self._read()
    def stat(self, msgid):
        try:
            r = self._cmd(f"STAT <{msgid}>")
            if not r:                                  # dropped -> reconnect once
                self.connect(); r = self._cmd(f"STAT <{msgid}>")
            return r.startswith("223")
        except Exception:
            try: self.connect()
            except Exception: pass
            return False
    def close(self):
        try: self._cmd("QUIT"); self.sock.close()
        except Exception: pass

def main():
    if not USER or not PASS:
        print("NO CREDS in", CONF); return
    con = sqlite3.connect(DB)
    rows = con.execute("SELECT id,name FROM torrents WHERE stored_nzb=1 AND deleted=0 "
                       "ORDER BY RANDOM() LIMIT ?", (SAMPLE,)).fetchall()
    nn = NNTP()
    full = partial = missing = nofetch = 0; art_ok = art_tot = 0
    t0 = time.time()
    for tid, name in rows:
        try:
            req = urllib.request.Request(nzb_url(tid, name),
                                         headers={"User-Agent": "Mozilla/5.0", "Referer": "https://animetosho.org/"})
            xml = urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "ignore")
        except Exception:
            nofetch += 1; continue
        segs = re.findall(r"<segment[^>]*>([^<]+)</segment>", xml)
        if not segs:
            nofetch += 1; continue
        n = len(segs)
        idxs = sorted(set([0, n // 2, n - 1] + [random.randint(0, n - 1) for _ in range(PER)]))[:PER]
        present = sum(nn.stat(segs[i]) for i in idxs)
        art_ok += present; art_tot += len(idxs)
        if present == len(idxs): full += 1
        elif present > 0: partial += 1
        else: missing += 1
    nn.close()
    checked = len(rows) - nofetch
    print(f"provider: {HOST}:{PORT}")
    print(f"sampled {len(rows)} NZBs in {round(time.time()-t0)}s | FULL {full} | partial {partial} | "
          f"missing {missing} | nzb-fetch-fail {nofetch}")
    print(f"article presence: {art_ok}/{art_tot} ({100*art_ok//max(art_tot,1)}%)")
    print(f"=> fully-retrievable rate: {100*full//max(checked,1)}%  (full+partial: {100*(full+partial)//max(checked,1)}%)")

if __name__ == "__main__":
    main()
