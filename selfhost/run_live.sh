#!/bin/bash
# LIVE recovery test run — resolves done with `dump_resolver --live` (current seeders). Fresh
# ledger = clean test of the live torrent path. all-NVENC + skip-ship + Mongo-callback.
# SUPERVISED: nzb_farm has no internal supervisor, so restart it on a crash (it resumes via the
# ledger; a clean completion prints SUMMARY and exits 0 -> we stop).
cd /data
[ -f /data/callback.env ] && source /data/callback.env
: > /data/done_live.jsonl
export TODO_FILE=/data/todo_live.jsonl DONE_LEDGER=/data/done_live.jsonl
export NGPU=1 GPU_WORKERS_PER=3 CPU_WORKERS=0 MAXQ=12 DL_THREADS=8
export SHIP_HOST="" SHIP_DEST=/data/ship_local
export CALLBACK_URL="${CALLBACK_URL}" CALLBACK_TOKEN="${SELFHOST_INGEST_TOKEN}"
for attempt in $(seq 1 8); do
  python3 nzb_farm.py && { echo "[run] farm completed normally"; break; }
  echo "[run] farm exited non-zero (attempt $attempt) — restart in 10s (resume via ledger)"; sleep 10
done
