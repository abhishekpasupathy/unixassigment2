#!/bin/sh
# End-to-end demonstration: both client classes, then an unexpected worker exit.
set -eu
cd "$(dirname "$0")"
make
./mq_router cleanup >/dev/null 2>&1 || true
./mq_router router > router.log 2>&1 &
ROUTER_PID=$!
cleanup() {
  kill -TERM "$ROUTER_PID" 2>/dev/null || true
  wait "$ROUTER_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM
sleep 1
printf '\n--- Standard client ---\n'
./mq_router client standard "Generate monthly report"
printf '\n--- Priority client ---\n'
./mq_router client priority "Process urgent payment"
printf '\n--- Crash the standard worker ---\n'
./mq_router client standard CRASH || true
sleep 1
printf '\n--- Request after standard worker crash (must be rejected) ---\n'
./mq_router client standard "This must not reach a dead worker" || true
printf '\n--- Priority worker must remain available ---\n'
./mq_router client priority "Priority worker still operational"
printf '\n--- Router log ---\n'
cat router.log
