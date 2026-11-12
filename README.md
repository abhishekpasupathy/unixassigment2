# System V Message-Queue Router with Request Isolation

A single-file C implementation of a multi-client-class router built with **System V IPC message queues**. It routes standard, priority and batch requests to independent worker processes, correlates every response with a request ID, validates untrusted messages, tracks per-client statistics and contains worker failures.

## Features

- Three independently supervised worker classes: `STANDARD`, `PRIORITY` and `BATCH`.
- The request queue's System V `mtype` identifies the client class: `1 = STANDARD`, `2 = PRIORITY`, `3 = BATCH`.
- Priority requests are polled by their exact type before the router uses a negative `msgtyp` to drain the remaining known classes. This preserves the requested type numbers while respecting System V selection semantics (a negative `msgtyp` selects the *lowest* message type not greater than its absolute value).
- Unexpected positive message types are consumed by a final type-zero receive and rejected instead of remaining stuck in the queue.
- Validation of message length, magic, protocol version, type/class agreement, client PID, request ID, reply queue ID, payload length and NUL termination.
- Request isolation through a router-owned pending table keyed by request ID. Worker responses do not carry a reply-queue ID; the router looks up the original reply queue and client PID in that table.
- Non-blocking queue sends, explicit `QUEUE_FULL` and `QUEUE_REMOVED` handling, and a bounded client response timeout.
- Per-client statistics keyed by client PID: received, forwarded, completed, rejected and failed. Unidentifiable malformed messages increment a separate global counter.
- `SIGUSR1` prints a live statistics snapshot. `Ctrl+C` or `SIGTERM` performs graceful shutdown.
- Worker supervision with `waitpid()`. A worker exit fails all pending requests assigned to that worker; only that class is disabled.
- `SLOW` test payload to hold a worker for two seconds and make queue saturation reproducible.
- `prctl(PR_SET_PDEATHSIG, SIGTERM)` in each worker. If the router is killed with `SIGKILL`, workers remove their own queue and the shared request/response queues before exiting.
- IPC permissions are restricted to `0600`.

## Architecture

```text
                      REQUEST QUEUE (mtype = class)
 +----------------+    +-----------------------------------------+
 | STANDARD client|--->| mtype 1                                  |
 | private reply Q|<---|                                         |
 +----------------+    |                                         |
                       |              ROUTER                     |
 +----------------+    | validate -> route -> pending table      |
 | PRIORITY client|--->| mtype 2                                  |
 | private reply Q|<---|                                         |
 +----------------+    |                                         |
                       |                                         |
 +----------------+    | mtype 3                                  |
 | BATCH client   |--->|                                         |
 | private reply Q|<---|                                         |
 +----------------+    +--------------------+--------------------+
                                            |
                   +------------------------+-----------------------+
                   |                        |                       |
             STANDARD Q                PRIORITY Q                BATCH Q
                   |                        |                       |
             +-----v------+           +-----v------+           +----v-------+
             | STD worker |           | PRI worker |           | BATCH worker|
             +-----+------+           +-----+------+           +-----+------+
                   |                        |                       |
                   +------------------------+-----------------------+
                                            |
                                     RESPONSE QUEUE
                                            |
                                 router matches request ID
                                 to pending-table entry
                                            |
                                  original private reply Q
```

### Request and response lifecycle

1. A client opens the well-known router queue and creates a private reply queue with `IPC_PRIVATE`.
2. It sends a request whose `mtype` identifies its class and whose body contains the same class ID, client PID, request ID, reply queue ID and validated payload metadata.
3. The router receives priority type `2` first. It then uses `msgrcv(..., -max_type, ...)` to drain the lowest remaining known type, and a type-zero receive to consume any unexpected type.
4. The router validates the message, reserves a pending-table entry keyed by request ID, and forwards the work to the selected worker queue.
5. The worker returns only the class and request ID on the response queue. It cannot choose or replace the destination reply queue.
6. The router finds the pending entry and sends the response to the reply queue recorded when that request was accepted. The pending entry is then cleared.
7. The client checks both the protocol marker and its request ID before accepting the response, then removes its private queue.

### Why priority is polled separately

System V `msgrcv()` with a negative `msgtyp` selects the message with the **lowest** type value that is less than or equal to the absolute value. With the required mapping (`1 = STANDARD`, `2 = PRIORITY`, `3 = BATCH`), a single `msgtyp = -3` call would select standard before priority. The router therefore polls exact type `2` first, then uses the negative receive for the remaining class types. This gives priority requests precedence without changing the specified type assignments.

## Project structure

```text
unixassigment2/
├── mq_router.c   # Router, worker processes, clients, protocol and test command
├── Makefile      # Build, test, demo, ASan/UBSan and clean targets
├── demo.sh       # End-to-end queue, permission and worker-failure demonstration
└── README.md     # Architecture, protocol and operating instructions
```

## Requirements

- Linux with System V IPC and `prctl(PR_SET_PDEATHSIG)`.
- GCC with C11 support.
- `make`, `ipcrm`, `xargs`; the permission demonstration additionally uses `runuser` and a `nobody` account.

## Build

```bash
make
```

The default build uses warnings as errors:

```bash
gcc -std=c11 -Wall -Wextra -Wpedantic -Werror -O2 -o mq_router mq_router.c
```

AddressSanitizer and UndefinedBehaviorSanitizer build:

```bash
make asan
ASAN_OPTIONS=detect_leaks=0 ./mq_router_asan router
```

## Run manually

Start the router in terminal 1:

```bash
./mq_router router
```

It prints its PID, request queue ID, response queue ID, and each worker's PID, queue ID and message type. In terminal 2:

```bash
./mq_router client standard "Generate monthly report"
./mq_router client priority "Process urgent payment"
./mq_router client batch "Archive daily records"
```

A worker's `SLOW` payload sleeps for two seconds and is intended for queue-pressure demonstrations:

```bash
./mq_router client standard SLOW
```

### Statistics

Send `SIGUSR1` to the router PID shown at startup:

```bash
kill -USR1 <router-pid>
```

The router prints one row per client PID with `received`, `forwarded`, `completed`, `rejected` and `failed` counters, plus `unidentified_rejections`. The same report is printed during shutdown.

### Malformed-message tests

With the router running, execute:

```bash
./mq_router test
```

The command submits seven deliberately invalid messages and waits for the router's rejection responses:

1. Wrong magic value.
2. Wrong protocol version.
3. Unknown class ID.
4. Short message.
5. Oversized message.
6. Missing NUL terminator.
7. Unexpected message type (`mtype = 99`).

The test command prints a confirmation for each rejection received. The router also logs the reason; the unexpected message type increments the global unidentified-rejection counter.

## Complete demonstration

Run:

```bash
make demo
```

`demo.sh` performs these steps in order:

1. Successfully routes one standard, one priority and one batch request.
2. Runs all seven malformed-message cases.
3. If run as root with `runuser`, attempts a client request as `nobody` and demonstrates `EACCES` against the mode-`0600` router queue.
4. Starts a `SLOW` standard request, then launches 120 concurrent clients. The small worker queue fills and excess clients receive `QUEUE_FULL`; accepted requests finish normally.
5. Removes the batch worker queue with `ipcrm -q`. The router reports `QUEUE_REMOVED` for that class while standard and priority continue to work.
6. Starts a `SLOW` priority request, reads the priority worker PID from the router log, and kills that worker externally with `kill -9`. The in-flight request receives `WORKER_FAILED`; later priority requests receive `WORKER_UNAVAILABLE`.
7. Confirms standard still works, requests a final statistics snapshot with `SIGUSR1`, then shuts down the router.

The demo writes `router.log`, `queue-full.log` and supporting test logs. `make clean` removes the executable, sanitizer executable and generated logs.

## Error handling

| Condition | Router/client behavior |
|---|---|
| Short, oversized, malformed or invalid protocol message | Receive buffer has one extra byte; message is rejected without writing beyond the buffer. The test command exercises both size boundaries. |
| Wrong magic/version, class mismatch, invalid payload or missing NUL | Reject and increment the identifiable client's rejected counter. |
| Unknown `mtype` | Consume with the type-zero receive, reject, and increment `unidentified_rejections` if the client class cannot be identified. |
| Worker queue full (`EAGAIN`) | Return `QUEUE_FULL`; the router does not block waiting for queue capacity. |
| Worker queue removed (`EIDRM` / `EINVAL`) | Mark that class unavailable and return `QUEUE_REMOVED`. |
| Worker exits unexpectedly | `waitpid(WNOHANG)` detects the exit; every pending request for that class receives `WORKER_FAILED`; later requests receive `WORKER_UNAVAILABLE`. Other classes remain available. |
| Client reply queue removed | Router logs delivery failure and clears the pending entry; other clients continue. |
| Client receives no response | Client times out after 12 seconds and removes its private reply queue. |
| Router killed with `SIGKILL` | Workers receive `SIGTERM` through `PR_SET_PDEATHSIG`; each worker removes its own queue and the shared request/response queues before exiting. |
| Queue access from another user | Mode `0600` prevents access; `msgget()` fails with `EACCES`. |

## IPC design and permissions

| Queue | Creation | Permissions | Purpose |
|---|---|---:|---|
| Router request queue | Fixed key `0x52545232` with `IPC_CREAT | IPC_EXCL` | `0600` | Client-to-router requests; `mtype` is the client class. |
| Worker input queues | `IPC_PRIVATE` | `0600` | One independent queue per worker class. Queue capacity is set to 1024 bytes for a repeatable full-queue demonstration. |
| Router response queue | `IPC_PRIVATE` | `0600` | Worker-to-router responses. |
| Client reply queues | `IPC_PRIVATE` | `0600` | Private destination for each client request. |

The router removes its owned queues with `msgctl(..., IPC_RMID, ...)` during graceful shutdown. Each client removes its own reply queue. Workers also clean up their IPC queues on parent death, covering the `SIGKILL` case. Worker and response queue IDs are printed at startup to support manual inspection and recovery.

## Cleanup

Graceful shutdown:

```bash
kill -TERM <router-pid>
```

Remove a stale well-known request queue when no router is active:

```bash
./mq_router cleanup
```

If the host terminates the router and all its workers before their parent-death handlers can run, inspect remaining queues with `ipcs -q` and remove an individual queue by ID using `ipcrm -q <queue-id>`.

## Learning outcomes covered

- **CO4 / CO5:** Inter-process communication, process coordination, error handling and fault isolation.
- **PO2:** Analyze and validate incoming requests.
- **PO3:** Design a modular, table-driven routing solution.
- **PO5:** Apply System V IPC, message types, permissions and Unix process-management tools.
- **PO9:** Demonstrate independent client/worker interaction and coordination.
- **SDG 9:** Reliable infrastructure through queue-based routing and failure handling.
- **SDG 16:** Controlled access through IPC permissions, validation and isolation.