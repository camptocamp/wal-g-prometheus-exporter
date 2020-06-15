#!/bin/bash

rm /tmp/full-backup.lock 2> /dev/null | true

while true; do
  sleep 20
  if [ ! -f /tmp/full-backup.lock ]; then
    touch /tmp/full-backup.lock
    wal-g backup-push $PGDATA
    rm /tmp/full-backup.lock
  fi
done
