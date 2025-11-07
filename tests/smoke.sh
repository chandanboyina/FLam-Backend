#!/usr/bin/env bash
set -euo pipefail

DB="smoke.db"
rm -f "$DB"

echo "== init status =="
queuectl --db "$DB" status || true

echo "== set config =="
queuectl --db "$DB" config set base_backoff 2
queuectl --db "$DB" config set job_timeout_sec 5

echo "== enqueue =="
queuectl --db "$DB" enqueue '{"id":"ok1","command":"echo hello","max_retries":3}'
queuectl --db "$DB" enqueue '{"id":"fail1","command":"bash -c \"exit 1\"","max_retries":2}'
queuectl --db "$DB" enqueue '{"id":"badcmd","command":"__no_such_command__","max_retries":1}'

echo "== start workers (2) =="
( queuectl --db "$DB" worker start --count 2 & ) 
sleep 1

echo "== observe =="
for i in {1..6}; do
  queuectl --db "$DB" status
  sleep 2
done

echo "== stop workers =="
queuectl worker stop
sleep 1

echo "== DLQ =="
queuectl --db "$DB" dlq list

echo "== retry from DLQ (if any) =="
DLQ_IDS=$(queuectl --db "$DB" dlq list | awk 'NR>3 {print $1}')
for id in $DLQ_IDS; do
  queuectl --db "$DB" dlq retry "$id"
done

echo "== restart workers to reprocess retried DLQ =="
( queuectl --db "$DB" worker start --count 1 & )
sleep 4
queuectl worker stop

echo "== final =="
queuectl --db "$DB" status
queuectl --db "$DB" list
