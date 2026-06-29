#!/bin/bash
while true; do
  if ! pidof nzbget >/dev/null; then
    rm -f /data/nzbget/nzbget.lock          # clear stale lock from a crashed/zombied nzbget so -D can restart
    /usr/bin/nzbget -c /data/nzbget.conf -D
  fi
  sleep 5
done
