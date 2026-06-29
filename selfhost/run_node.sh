#!/bin/bash
# Per-node partition runner for the multi-node fill -> offshore. Each node processes its own
# /data/todo_node.jsonl (disjoint anime), ships to offshore, marks Mongo. all-NVENC, supervised.
# Per-node overrides in /data/node.env (MAXQ, DL_THREADS, GPU_WORKERS_PER, GUARD_KB) — used to
# bound the disk footprint on tight hosts (e.g. canada-1 = shared goongle prod, ~40GB free).
# Dual disk-guard: stop if offshore /srv <1.5TB free (16TB cap) OR local /data < GUARD_KB free.
cd /data
[ -f /data/callback.env ] && source /data/callback.env
[ -f /data/node.env ] && source /data/node.env
OFF=root@185.255.120.59
GUARD_KB=${GUARD_KB:-52428800}     # default 50GB local floor
( while sleep 180; do
    a=$(ssh -o BatchMode=yes -o ConnectTimeout=10 $OFF "df --output=avail -k /srv|tail -1" 2>/dev/null|tr -d ' ')
    [ -n "$a" ] && [ "$a" -lt 1610612736 ] && { echo "[mon $(date +%H:%M)] offshore<1.5TB -> STOP"; pkill -9 -f nzb_farm.py; break; }
    l=$(df --output=avail -k /data 2>/dev/null|tail -1|tr -d ' ')
    [ -n "$l" ] && [ "$l" -lt "$GUARD_KB" ] && { echo "[mon $(date +%H:%M)] local /data<${GUARD_KB}KB -> STOP"; pkill -9 -f nzb_farm.py; break; }
  done ) >/data/disk_monitor.log 2>&1 &
export TODO_FILE=/data/todo_node.jsonl DONE_LEDGER=/data/done_node.jsonl
export NGPU=1 GPU_WORKERS_PER=${GPU_WORKERS_PER:-3} CPU_WORKERS=0 MAXQ=${MAXQ:-12} DL_THREADS=${DL_THREADS:-6}
export SHIP_HOST=$OFF SHIP_PORT=22 SHIP_DEST=/srv/hls
export CALLBACK_URL="${CALLBACK_URL}" CALLBACK_TOKEN="${SELFHOST_INGEST_TOKEN}"
while true; do
  python3 nzb_farm.py && { echo "[run] completed — holding session idle"; sleep infinity; }
  echo "[run] farm exited non-zero — restart 15s"; sleep 15
done
