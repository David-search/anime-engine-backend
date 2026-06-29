#!/bin/bash
# AniChan self-host build+ship runner (the production entrypoint).
#
#   resolve (AnimeTosho dump, offline)  ->  download (NZB primary / torrent fallback /
#   batch-pack)  ->  Y-mode encode  ->  ship to origin  ->  mark Mongo (selfhost_cache).
#
# Resumable: DONE_LEDGER skips already-built episodes (safe to re-run / restart).
# Bounded disk: ship-and-delete. Coverage: POSTs /api/watch/cache-state per anime.
#
# TEST (now):  ships to vast-canada-2, marks canada-2 backend Mongo.
# PROD (CDN ready): set SHIP_HOST + SHIP_DEST to the real origin, then run with --top 1000.
#
# Examples:
#   TITLES_FILE=/data/titles_10.txt SHIP_DEST=/data/ship_test_run \
#     CALLBACK_URL=http://70.30.158.46:43577/api/watch bash selfhost_run.sh
#   N_ANIME=1000 SHIP_HOST=root@<origin> SHIP_DEST=/srv/hls bash selfhost_run.sh
set -u
cd /data
LOG="${LOG:-/data/selfhost_run.log}"; : > "$LOG"
say(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }

# ---------- config (override via env) ----------
TITLES_FILE="${TITLES_FILE:-}"                  # curated title list; else top-N popular
N_ANIME="${N_ANIME:-1000}"
SHIP_HOST="${SHIP_HOST:-root@70.30.158.46}"     # PROD: change to real origin host
SHIP_PORT="${SHIP_PORT:-43730}"
SHIP_DEST="${SHIP_DEST:-/data/anichan}"         # PROD: change to origin's HLS serve path
NGPU="${NGPU:-4}"; GPU_WORKERS_PER="${GPU_WORKERS_PER:-1}"; CPU_WORKERS="${CPU_WORKERS:-8}"; MAXQ="${MAXQ:-10}"
TODO="${TODO:-/data/todo_run.jsonl}"
DONE_LEDGER="${DONE_LEDGER:-/data/done.jsonl}"
# Mongo coverage callback: backend /api/watch/cache-state. Token from /data/callback.env if present.
[ -f /data/callback.env ] && source /data/callback.env
CALLBACK_URL="${CALLBACK_URL:-}"
CALLBACK_TOKEN="${CALLBACK_TOKEN:-${SELFHOST_INGEST_TOKEN:-}}"

# ---------- prerequisites ----------
pgrep -x transmission-da >/dev/null || (transmission-daemon --config-dir /data/transmission --download-dir /data/library --no-auth 2>/dev/null; sleep 2)
grep -q "^DupeCheck=no" /data/nzbget.conf 2>/dev/null || echo "DupeCheck=no" >> /data/nzbget.conf
pgrep -x nzbget >/dev/null || (nzbget -c /data/nzbget.conf -D 2>/dev/null; sleep 3)

# ---------- resolve ----------
if [ -n "$TITLES_FILE" ]; then
  say "RESOLVE titles=$TITLES_FILE"
  python3 dump_resolver.py --out "$TODO" --titles-file "$TITLES_FILE" --allow-hevc 1 2>&1 | tee -a "$LOG"
else
  say "RESOLVE top=$N_ANIME"
  python3 dump_resolver.py --out "$TODO" --top "$N_ANIME" --allow-hevc 1 2>&1 | tee -a "$LOG"
fi
say "todo: $(wc -l < "$TODO" 2>/dev/null || echo 0) items"

# ---------- farm ----------
say "FARM -> ${SHIP_HOST}:${SHIP_DEST}  (Mongo callback: ${CALLBACK_URL:-OFF})"
TODO_FILE="$TODO" DONE_LEDGER="$DONE_LEDGER" NGPU="$NGPU" GPU_WORKERS_PER="$GPU_WORKERS_PER" \
  CPU_WORKERS="$CPU_WORKERS" MAXQ="$MAXQ" CPU_PRESET=veryfast \
  SHIP_HOST="$SHIP_HOST" SHIP_PORT="$SHIP_PORT" SHIP_DEST="$SHIP_DEST" \
  CALLBACK_URL="$CALLBACK_URL" CALLBACK_TOKEN="$CALLBACK_TOKEN" \
  python3 nzb_farm.py 2>&1 | tee -a "$LOG"
say "RUN COMPLETE"
