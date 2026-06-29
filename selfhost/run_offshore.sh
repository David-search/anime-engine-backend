#!/bin/bash
# Production run -> OFFSHORE origin (185.255.120.59:/srv/hls). Ship target is the real HLS origin
# the backend (SELFHOST_ORIGIN) probes + proxies. all-NVENC, supervised, marks Mongo coverage.
cd /data
[ -f /data/callback.env ] && source /data/callback.env
: > /data/done_offshore.jsonl
export TODO_FILE=/data/todo_offshore.jsonl DONE_LEDGER=/data/done_offshore.jsonl
export NGPU=1 GPU_WORKERS_PER=3 CPU_WORKERS=0 MAXQ=12 DL_THREADS=8
export SHIP_HOST=root@185.255.120.59 SHIP_PORT=22 SHIP_DEST=/srv/hls
export CALLBACK_URL="${CALLBACK_URL}" CALLBACK_TOKEN="${SELFHOST_INGEST_TOKEN}"
for attempt in $(seq 1 8); do
  python3 nzb_farm.py && { echo "[run] farm completed normally"; break; }
  echo "[run] farm exited non-zero (attempt $attempt) — restart in 10s"; sleep 10
done
