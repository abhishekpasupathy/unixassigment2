#!/bin/sh
# Full System V queue router demonstration.
set -eu
cd "$(dirname "$0")"
make
./mq_router cleanup >/dev/null 2>&1 || true
rm -f router.log queue-full.log permission.log removed.log unavailable.log crash-client.log slow-client.log

./mq_router router > router.log 2>&1 &
ROUTER_PID=$!
cleanup() {
    kill -TERM "$ROUTER_PID" 2>/dev/null || true
    wait "$ROUTER_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

attempt=0
while ! grep -q '\[router\] running pid=' router.log; do
    attempt=$((attempt + 1))
    if [ "$attempt" -gt 100 ]; then
        echo "Router did not start; see router.log" >&2
        cat router.log >&2
        exit 1
    fi
    sleep 0.05
done

worker_pid() {
    awk -v name="$1" '$3 == name { for (i = 1; i <= NF; i++) if ($i ~ /^pid=/) { sub("pid=", "", $i); print $i; exit } }' router.log
}

worker_queue() {
    awk -v name="$1" '$3 == name { for (i = 1; i <= NF; i++) if ($i ~ /^queue=/) { sub("queue=", "", $i); print $i; exit } }' router.log
}

printf '\n=== 1. Normal routing: every client class ===\n'
./mq_router client standard "Standard report"
./mq_router client priority "Priority payment"
./mq_router client batch "Batch archive"

printf '\n=== 2. Malformed request rejection ===\n'
./mq_router test

printf '\n=== Permission check: IPC mode 0600 ===\n'
if [ "$(id -u)" -eq 0 ] && command -v runuser >/dev/null 2>&1; then
    PERMISSION_BINARY="/tmp/mq-router-permission-$$"
    cp ./mq_router "$PERMISSION_BINARY"
    chmod 755 "$PERMISSION_BINARY"
    if runuser -u nobody -- "$PERMISSION_BINARY" client standard "permission check" > permission.log 2>&1; then
        echo "ERROR: unprivileged client unexpectedly accessed the router queue" >&2
        rm -f "$PERMISSION_BINARY"
        exit 1
    fi
    if grep -q 'Permission denied' permission.log; then
        echo "PASS: user nobody received EACCES for the 0600 router queue"
        cat permission.log
    else
        echo "Permission check did not report EACCES:" >&2
        cat permission.log >&2
        rm -f "$PERMISSION_BINARY"
        exit 1
    fi
    rm -f "$PERMISSION_BINARY"
else
    echo "SKIP: run this demo as root with runuser installed to demonstrate EACCES."
fi

printf '\n=== 3. Full worker queue ===\n'
./mq_router client standard SLOW > slow-client.log 2>&1 &
SLOW_PID=$!
sleep 0.1
seq 1 120 | xargs -P 120 -I '{}' sh -c './mq_router client standard "burst-{}" >> queue-full.log 2>&1 || true'
wait "$SLOW_PID"
QUEUE_FULL_COUNT=$(grep -c 'QUEUE_FULL' queue-full.log || true)
if [ "$QUEUE_FULL_COUNT" -eq 0 ]; then
    echo "ERROR: burst did not produce QUEUE_FULL" >&2
    cat queue-full.log >&2
    exit 1
fi
echo "PASS: $QUEUE_FULL_COUNT clients received QUEUE_FULL; accepted requests completed."

printf '\n=== 4. Removed worker queue ===\n'
BATCH_QUEUE=$(worker_queue BATCH)
ipcrm -q "$BATCH_QUEUE"
sleep 0.1
if ./mq_router client batch "request after queue removal" > removed.log 2>&1; then
    echo "ERROR: removed batch queue unexpectedly accepted a request" >&2
    exit 1
fi
if ! grep -q 'QUEUE_REMOVED' removed.log; then
    echo "ERROR: expected QUEUE_REMOVED response" >&2
    cat removed.log >&2
    exit 1
fi
cat removed.log
./mq_router client standard "standard unaffected by batch queue removal"
./mq_router client priority "priority unaffected by batch queue removal"

printf '\n=== 5. Kill a worker unexpectedly with SIGKILL ===\n'
./mq_router client priority SLOW > crash-client.log 2>&1 &
CRASH_CLIENT_PID=$!
sleep 0.2
PRIORITY_PID=$(worker_pid PRIORITY)
kill -9 "$PRIORITY_PID"
wait "$CRASH_CLIENT_PID" || true
if ! grep -q 'WORKER_FAILED' crash-client.log; then
    echo "ERROR: in-flight request did not receive WORKER_FAILED" >&2
    cat crash-client.log >&2
    exit 1
fi
cat crash-client.log
if ./mq_router client priority "request after worker crash" > unavailable.log 2>&1; then
    echo "ERROR: dead priority worker unexpectedly accepted a request" >&2
    exit 1
fi
cat unavailable.log
./mq_router client standard "standard still operational after priority crash"

printf '\n=== 6. Final per-client statistics ===\n'
kill -USR1 "$ROUTER_PID"
sleep 0.1
kill -TERM "$ROUTER_PID"
wait "$ROUTER_PID"
trap - EXIT INT TERM
cat router.log
printf '\nDemo complete. Detailed burst output: queue-full.log\n'