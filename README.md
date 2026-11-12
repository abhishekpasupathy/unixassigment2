# System V Message-Queue Router with Request Isolation

A C implementation of a multi-class request router using **System V IPC message queues**. The router accepts requests from standard and priority clients, validates the protocol, forwards each request to its matching worker queue, and returns the result to the requesting client using a private reply queue and a unique request ID.

## Features

- Separate routing paths for `STANDARD` and `PRIORITY` client classes.
- System V queues created and accessed with `msgget()`, `msgsnd()` and `msgrcv()`.
- Request protocol validation: message length, magic value, protocol version, client class, request ID, reply queue ID, payload length and terminating NUL byte.
- Request isolation: every client creates a private reply queue; every response carries the originating request ID. A client accepts only the response matching its own ID.
- Non-blocking queue operations so a full worker queue is reported instead of blocking the router.
- Explicit handling for full queues (`EAGAIN`), removed/invalid queues (`EIDRM`/`EINVAL`), and delivery failures.
- Per-client-class counters for received, forwarded, completed, rejected and failed requests.
- Worker supervision using `waitpid()`. An unexpected worker exit disables only that class; the other class continues working.
- Graceful router shutdown removes all queues owned by the router.

## Architecture

```text
                    System V IPC message queues

 +------------------+       +--------------------------+
 | STANDARD client  |------>|                          |
 | private reply Q  |<------+                          |
 +------------------+       |                          |
                            |       ROUTER PROCESS      |
 +------------------+       |                          |
 | PRIORITY client  |------>|  validate + classify     |
 | private reply Q  |<------+  route + account stats   |
 +------------------+       |                          |
                            +------------+-------------+
                                         |
                         +---------------+---------------+
                         |                               |
                  standard queue                  priority queue
                         |                               |
                  +------v------+                 +------v------+
                  | STANDARD    |                 | PRIORITY    |
                  | WORKER      |                 | WORKER      |
                  +------+------+                 +------+------+
                         |                               |
                         +---------------+---------------+
                                         |
                                  response queue
                                         |
                                  Router matches
                                  request ID and
                                  reply queue ID
                                         |
                              private client reply queue
```

### Request lifecycle

1. A client connects to the router's well-known request queue.
2. The client creates a private reply queue with `IPC_PRIVATE` and generates a request ID.
3. The router receives and validates the complete request structure.
4. The router selects the worker queue from the client class and forwards the request without blocking.
5. The worker processes the request and sends a response to the router's response queue.
6. The router forwards that response to the reply queue supplied by the originating client.
7. The client verifies the request ID before displaying the response and removes its private queue.

## Project structure

```text
unixassigment2/
├── mq_router.c   # Router, worker and client processes; shared IPC protocol
├── Makefile      # Build and clean targets
├── demo.sh       # End-to-end routing and worker-failure demonstration
└── README.md     # Documentation
```

## Requirements

- Linux or a Unix-like operating system providing System V IPC.
- GCC or another C11-compatible compiler.
- `make`.

## Compile

```bash
make
```

Equivalent direct compilation:

```bash
gcc -std=c11 -Wall -Wextra -Wpedantic -O2 -o mq_router mq_router.c
```

## Run

Open terminal 1 and start the router:

```bash
./mq_router router
```

Open terminal 2 (or additional terminals) and submit requests:

```bash
./mq_router client standard "Generate monthly report"
./mq_router client priority "Process urgent payment"
```

The router prints routing events and a per-class statistics summary when stopped with `Ctrl+C`.

## Demonstrate an unexpected worker exit

The special payload `CRASH` is a deliberate demonstration hook. The selected worker consumes the request and exits immediately without returning a response. The router reaps the child, reports the failure and marks only that worker class unavailable.

Run the complete demonstration:

```bash
./demo.sh
```

Expected behavior:

1. A standard request is processed by the standard worker.
2. A priority request is processed by the priority worker.
3. A standard `CRASH` request causes the standard worker to exit; its client times out because no response is produced.
4. A later standard request receives `WORKER_UNAVAILABLE` from the router.
5. A priority request still succeeds, proving failure isolation between worker classes.

The demo writes router output to `router.log` and prints it at the end.

## Error handling

| Condition | Handling |
|---|---|
| Invalid structure, protocol marker/version, class, request ID, or payload | Reject request; increment rejected counter when class can be identified |
| Payload is empty or exceeds the fixed protocol buffer | Client rejects it before sending; router independently validates payload length |
| Worker queue is full (`EAGAIN`) | Non-blocking send fails; router returns `QUEUE_FULL` |
| Worker queue was removed or became invalid (`EIDRM` / `EINVAL`) | Router returns a queue error rather than waiting indefinitely |
| Worker process exits | Router detects it with `waitpid(WNOHANG)` and disables that class |
| Client reply queue was removed | Router logs response-delivery failure; other requests continue |
| Client receives no response | Client times out after 10 seconds and removes its private reply queue |

## IPC design and permissions

- Router request queue: fixed key `0x52545232`, permissions `0600`.
- Worker input queues and router response queue: private queues created with `IPC_PRIVATE`, permissions `0600`.
- Client reply queues: private queues created with `IPC_PRIVATE`, permissions `0600`.
- The message `mtype` is always positive. The implementation uses a dedicated message type for each protocol queue and carries the request ID in the message body for explicit correlation.
- The router owns and removes its request, worker and response queues on graceful shutdown. Each client removes its own private reply queue.

## Cleanup

Stop the router with `Ctrl+C`. If a previous run was terminated forcefully and left the well-known request queue behind, run:

```bash
./mq_router cleanup
```

This removes only the well-known router request queue. It does not terminate a still-running router process; stop any active router before starting another one.

## Learning outcomes covered

- **CO4 / CO5:** Inter-process communication, process coordination, error handling and fault isolation.
- **PO2:** Analyze and validate incoming requests.
- **PO3:** Design a modular routing solution.
- **PO5:** Apply System V IPC and Unix process-management tools.
- **PO9:** Demonstrate independent client/worker interaction and coordination.
- **SDG 9:** Reliable infrastructure through queue-based routing and failure handling.
- **SDG 16:** Controlled access through IPC permissions, validation and isolation.

## Notes

This is an educational single-router demonstration. Request IDs are generated per client process and are used for response correlation; they are not a durable distributed transaction identifier. The fixed router key allows local clients to discover the router and should be changed if multiple independent instances must run under the same IPC namespace.
