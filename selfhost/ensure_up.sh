#!/bin/bash
# Per-node autonomy watchdog. Ensures the 3 tmux sessions exist; recreates any that died
# (supervisor crash, tmux death, or node reboot). Run from cron every 2 min + @reboot.
# The supervisors inside each session self-heal their daemons (nzbget clears its stale lock;
# trd restarts transmission; farm/run_node.sh retries nzb_farm.py forever).
export PATH=/usr/local/bin:/usr/bin:/bin
cd /data 2>/dev/null || exit 0
tmux has-session -t nzbget 2>/dev/null || tmux new-session -d -s nzbget "bash /data/nzbget_supervisor.sh"
tmux has-session -t trd 2>/dev/null || tmux new-session -d -s trd "while true; do pgrep -x transmission-da >/dev/null || transmission-daemon --download-dir /data/library; sleep 10; done"
tmux has-session -t farm 2>/dev/null || tmux new-session -d -s farm "bash /data/run_node.sh >>/data/run_node.log 2>&1"
