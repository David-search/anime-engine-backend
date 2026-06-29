#!/bin/bash
# TOP-N most-popular anime -> offshore origin (/srv/hls), capped under 16TB. Remux Y-mode,
# all-NVENC, supervised. DISK-MONITOR backstop: stops the farm if offshore drops below 1.5TB
# free (hard 16TB cap regardless of size-estimate error). Ledger NOT cleared -> resume + skip
# already-cached (seeded from offshore).
cd /data
[ -f /data/callback.env ] && source /data/callback.env
OFF=root@185.255.120.59
( while sleep 300; do
    avail=$(ssh -o BatchMode=yes -o ConnectTimeout=10 $OFF "df --output=avail -k /srv 2>/dev/null | tail -1" 2>/dev/null | tr -d ' ')
    if [ -n "$avail" ] && [ "$avail" -lt 1610612736 ]; then
      echo "[disk-monitor $(date +%H:%M)] offshore <1.5TB free ($avail KB) -> STOPPING farm"; pkill -9 -f nzb_farm.py; break
    fi
  done ) >/data/disk_monitor.log 2>&1 &
MON=$!
export TODO_FILE=/data/todo_top.jsonl DONE_LEDGER=/data/done_top.jsonl
export NGPU=1 GPU_WORKERS_PER=3 CPU_WORKERS=0 MAXQ=12 DL_THREADS=8
export SHIP_HOST=$OFF SHIP_PORT=22 SHIP_DEST=/srv/hls
export CALLBACK_URL="${CALLBACK_URL}" CALLBACK_TOKEN="${SELFHOST_INGEST_TOKEN}"
for attempt in $(seq 1 30); do
  python3 nzb_farm.py && { echo "[run] farm completed normally"; break; }
  echo "[run] farm exited non-zero (attempt $attempt) — restart in 15s"; sleep 15
done
kill $MON 2>/dev/null
