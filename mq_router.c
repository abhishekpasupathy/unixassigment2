#define _GNU_SOURCE
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/wait.h>
#include <sys/prctl.h>
#include <signal.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <time.h>
#include <limits.h>
#include <stddef.h>

#define ROUTER_KEY ((key_t)0x52545232)
#define MAGIC 0x4D515254u
#define VERSION 1u
#define PAYLOAD_SIZE 256
#define MAX_PENDING 8192
#define MAX_CLIENT_STATS 2048
#define CLIENT_TIMEOUT 12
#define CLASS_STANDARD 1u
#define CLASS_PRIORITY 2u
#define CLASS_BATCH 3u
#define STATUS_OK 0
#define STATUS_REJECTED 2
#define STATUS_UNAVAILABLE 3
#define STATUS_QUEUE_FULL 4
#define STATUS_QUEUE_REMOVED 5
#define STATUS_WORKER_FAILED 6
#define STATUS_ROUTING_ERROR 7

typedef struct {
    long mtype;
    unsigned magic;
    unsigned version;
    unsigned class_id;
    pid_t client_pid;
    unsigned long long request_id;
    int reply_queue;
    unsigned payload_len;
    char payload[PAYLOAD_SIZE];
} Request;

typedef struct {
    long mtype;
    unsigned magic;
    unsigned class_id;
    unsigned long long request_id;
    int status;
    char text[PAYLOAD_SIZE];
} WorkResponse;

typedef struct {
    long mtype;
    unsigned magic;
    unsigned class_id;
    unsigned long long request_id;
    unsigned payload_len;
    char payload[PAYLOAD_SIZE];
} WorkRequest;

typedef struct {
    long mtype;
    unsigned magic;
    unsigned long long request_id;
    int status;
    char text[PAYLOAD_SIZE];
} ClientResponse;

typedef struct {
    unsigned class_id;
    const char *name;
    long message_type;
    int queue_id;
    pid_t worker_pid;
    int alive;
    int queue_removed;
} WorkerClass;

typedef struct {
    int used;
    unsigned long long request_id;
    int reply_queue;
    pid_t client_pid;
    unsigned class_id;
} PendingRequest;

typedef struct {
    int used;
    pid_t pid;
    unsigned long received;
    unsigned long forwarded;
    unsigned long completed;
    unsigned long rejected;
    unsigned long failed;
} ClientStats;

static WorkerClass workers[] = {
    { CLASS_STANDARD, "STANDARD", 1L, -1, -1, 0, 0 },
    { CLASS_PRIORITY, "PRIORITY", 2L, -1, -1, 0, 0 },
    { CLASS_BATCH, "BATCH", 3L, -1, -1, 0, 0 }
};
#define WORKER_COUNT (sizeof(workers) / sizeof(workers[0]))
static int request_queue = -1;
static int response_queue = -1;
static PendingRequest pending[MAX_PENDING];
static ClientStats client_stats[MAX_CLIENT_STATS];
static unsigned long unidentified_rejections;
static volatile sig_atomic_t shutdown_requested;
static volatile sig_atomic_t stats_requested;
static volatile sig_atomic_t worker_shutdown;

typedef union {
    Request request;
    unsigned char bytes[sizeof(Request) + 1];
} RequestBuffer;

typedef struct {
    long mtype;
    unsigned char bytes[sizeof(Request) + 512];
} RawMessage;

static void handle_worker_sigterm(int signal_number)
{
    (void)signal_number;
    worker_shutdown = 1;
}

static void handle_signal(int signal_number)
{
    if (signal_number == SIGUSR1) {
        stats_requested = 1;
    } else {
        shutdown_requested = 1;
    }
}

static void sleep_milliseconds(long milliseconds)
{
    struct timespec delay;
    delay.tv_sec = milliseconds / 1000;
    delay.tv_nsec = (milliseconds % 1000) * 1000000L;
    while (nanosleep(&delay, &delay) < 0 && errno == EINTR) {
    }
}

static WorkerClass *worker_for_class(unsigned class_id)
{
    size_t index;
    for (index = 0; index < WORKER_COUNT; index++) {
        if (workers[index].class_id == class_id) {
            return &workers[index];
        }
    }
    return NULL;
}

static WorkerClass *worker_for_type(long message_type)
{
    size_t index;
    for (index = 0; index < WORKER_COUNT; index++) {
        if (workers[index].message_type == message_type) {
            return &workers[index];
        }
    }
    return NULL;
}

static ClientStats *stats_for_pid(pid_t pid, int create)
{
    size_t index;
    ClientStats *free_slot = NULL;
    for (index = 0; index < MAX_CLIENT_STATS; index++) {
        if (client_stats[index].used && client_stats[index].pid == pid) {
            return &client_stats[index];
        }
        if (!client_stats[index].used && free_slot == NULL) {
            free_slot = &client_stats[index];
        }
    }
    if (!create || free_slot == NULL || pid <= 0) {
        return NULL;
    }
    memset(free_slot, 0, sizeof(*free_slot));
    free_slot->used = 1;
    free_slot->pid = pid;
    return free_slot;
}

static PendingRequest *find_pending(unsigned long long request_id)
{
    size_t index;
    for (index = 0; index < MAX_PENDING; index++) {
        if (pending[index].used && pending[index].request_id == request_id) {
            return &pending[index];
        }
    }
    return NULL;
}

static PendingRequest *reserve_pending(const Request *request)
{
    size_t index;
    if (find_pending(request->request_id) != NULL) {
        return NULL;
    }
    for (index = 0; index < MAX_PENDING; index++) {
        if (!pending[index].used) {
            pending[index].used = 1;
            pending[index].request_id = request->request_id;
            pending[index].reply_queue = request->reply_queue;
            pending[index].client_pid = request->client_pid;
            pending[index].class_id = request->class_id;
            return &pending[index];
        }
    }
    return NULL;
}

static void clear_pending(PendingRequest *entry)
{
    if (entry != NULL) {
        memset(entry, 0, sizeof(*entry));
    }
}

static void send_client_response(int reply_queue, unsigned long long request_id,
                                 int status, const char *message)
{
    ClientResponse response;
    memset(&response, 0, sizeof(response));
    response.mtype = 1L;
    response.magic = MAGIC;
    response.request_id = request_id;
    response.status = status;
    snprintf(response.text, sizeof(response.text), "%s", message);
    if (msgsnd(reply_queue, &response, sizeof(response) - sizeof(long), IPC_NOWAIT) < 0) {
        fprintf(stderr, "[router] reply delivery failed for %llu: %s\n",
                request_id, strerror(errno));
    }
}

static void deliver_response(const WorkResponse *worker_response)
{
    PendingRequest *entry = find_pending(worker_response->request_id);
    ClientStats *stats;
    if (entry == NULL || entry->class_id != worker_response->class_id) {
        fprintf(stderr, "[router] discarded unmatched worker response id=%llu\n",
                worker_response->request_id);
        return;
    }
    send_client_response(entry->reply_queue, entry->request_id,
                         worker_response->status, worker_response->text);
    stats = stats_for_pid(entry->client_pid, 0);
    if (stats != NULL) {
        stats->completed++;
    }
    clear_pending(entry);
    printf("[router] response delivered id=%llu\n", worker_response->request_id);
}

static void fail_pending_for_worker(WorkerClass *worker, int status, const char *reason)
{
    size_t index;
    for (index = 0; index < MAX_PENDING; index++) {
        PendingRequest *entry = &pending[index];
        ClientStats *stats;
        if (!entry->used || entry->class_id != worker->class_id) {
            continue;
        }
        send_client_response(entry->reply_queue, entry->request_id, status, reason);
        stats = stats_for_pid(entry->client_pid, 0);
        if (stats != NULL) {
            stats->failed++;
        }
        clear_pending(entry);
    }
}

static void print_stats(void)
{
    size_t index;
    printf("\n========== PER-CLIENT STATISTICS ==========\n");
    printf("PID       RECEIVED  FORWARDED  COMPLETED  REJECTED  FAILED\n");
    for (index = 0; index < MAX_CLIENT_STATS; index++) {
        const ClientStats *stats = &client_stats[index];
        if (!stats->used) {
            continue;
        }
        printf("%-9ld %-9lu %-10lu %-10lu %-9lu %-9lu\n",
               (long)stats->pid, stats->received, stats->forwarded,
               stats->completed, stats->rejected, stats->failed);
    }
    printf("unidentified_rejections=%lu\n", unidentified_rejections);
    fflush(stdout);
}

static int validate_request(const Request *request, ssize_t received_size,
                            WorkerClass **worker_out, const char **reason_out)
{
    WorkerClass *worker = worker_for_type(request->mtype);
    *worker_out = worker;
    *reason_out = "malformed request";
    if (received_size != (ssize_t)(sizeof(Request) - sizeof(long))) {
        *reason_out = "invalid message size";
        return 0;
    }
    if (worker == NULL) {
        *reason_out = "unexpected message type";
        return 0;
    }
    if (request->magic != MAGIC) {
        *reason_out = "invalid magic";
        return 0;
    }
    if (request->version != VERSION) {
        *reason_out = "unsupported protocol version";
        return 0;
    }
    if (request->class_id != worker->class_id) {
        *reason_out = "message type/class mismatch";
        return 0;
    }
    if (request->client_pid <= 0 || request->request_id == 0 || request->reply_queue < 0) {
        *reason_out = "invalid client identity or reply queue";
        return 0;
    }
    if (request->payload_len < 1 || request->payload_len > PAYLOAD_SIZE) {
        *reason_out = "invalid payload length";
        return 0;
    }
    if (request->payload[request->payload_len - 1] != '\0' ||
        strnlen(request->payload, request->payload_len) != request->payload_len - 1) {
        *reason_out = "payload is not NUL terminated";
        return 0;
    }
    return 1;
}

static void route_request(const Request *request, WorkerClass *worker)
{
    ClientStats *stats = stats_for_pid(request->client_pid, 1);
    PendingRequest *entry;
    WorkRequest work;
    if (stats != NULL) {
        stats->received++;
    }
    if (!worker->alive) {
        if (stats != NULL) {
            stats->failed++;
        }
        send_client_response(request->reply_queue, request->request_id,
                             worker->queue_removed ? STATUS_QUEUE_REMOVED : STATUS_UNAVAILABLE,
                             worker->queue_removed ? "QUEUE_REMOVED" : "WORKER_UNAVAILABLE");
        return;
    }
    entry = reserve_pending(request);
    if (entry == NULL) {
        if (stats != NULL) {
            stats->failed++;
        }
        send_client_response(request->reply_queue, request->request_id,
                             STATUS_ROUTING_ERROR, "PENDING_TABLE_FULL_OR_DUPLICATE_ID");
        return;
    }
    memset(&work, 0, sizeof(work));
    work.mtype = 1L;
    work.magic = MAGIC;
    work.class_id = request->class_id;
    work.request_id = request->request_id;
    work.payload_len = request->payload_len;
    memcpy(work.payload, request->payload, request->payload_len);
    if (msgsnd(worker->queue_id, &work, sizeof(work) - sizeof(long), IPC_NOWAIT) < 0) {
        int saved_errno = errno;
        clear_pending(entry);
        if (stats != NULL) {
            stats->failed++;
        }
        if (saved_errno == EIDRM || saved_errno == EINVAL) {
            worker->alive = 0;
            worker->queue_removed = 1;
            fail_pending_for_worker(worker, STATUS_QUEUE_REMOVED, "QUEUE_REMOVED");
            send_client_response(request->reply_queue, request->request_id,
                                 STATUS_QUEUE_REMOVED, "QUEUE_REMOVED");
            fprintf(stderr, "[router] %s worker queue removed; class disabled\n", worker->name);
        } else if (saved_errno == EAGAIN) {
            send_client_response(request->reply_queue, request->request_id,
                                 STATUS_QUEUE_FULL, "QUEUE_FULL");
            fprintf(stderr, "[router] %s worker queue full\n", worker->name);
        } else {
            send_client_response(request->reply_queue, request->request_id,
                                 STATUS_ROUTING_ERROR, strerror(saved_errno));
        }
        return;
    }
    if (stats != NULL) {
        stats->forwarded++;
    }
    printf("[router] request %llu -> %s (client pid=%ld)\n",
           request->request_id, worker->name, (long)request->client_pid);
}

static void reject_request(const Request *request, WorkerClass *worker,
                           const char *reason, int known_size)
{
    ClientStats *stats = NULL;
    if (worker != NULL && request->client_pid > 0) {
        stats = stats_for_pid(request->client_pid, 1);
    }
    if (stats != NULL) {
        stats->received++;
        stats->rejected++;
    } else {
        unidentified_rejections++;
    }
    fprintf(stderr, "[router] rejected malformed request: %s\n", reason);
    if (known_size && request->request_id != 0 && request->reply_queue >= 0) {
        send_client_response(request->reply_queue, request->request_id,
                             STATUS_REJECTED, reason);
    }
}

static ssize_t receive_one_request(RequestBuffer *buffer)
{
    ssize_t received;
    memset(buffer, 0, sizeof(*buffer));
    WorkerClass *priority_worker = worker_for_class(CLASS_PRIORITY);
    long highest_message_type = 0;
    size_t index;
    for (index = 0; index < WORKER_COUNT; index++) {
        if (workers[index].message_type > highest_message_type) {
            highest_message_type = workers[index].message_type;
        }
    }
    /* Poll priority's exact type first. The negative receive then selects the
       lowest remaining class type, as defined by System V msgrcv semantics. */
    received = msgrcv(request_queue, &buffer->request,
                      sizeof(Request) - sizeof(long) + 1,
                      priority_worker->message_type,
                      IPC_NOWAIT | MSG_NOERROR);
    if (received >= 0 || (errno != ENOMSG && errno != EINTR)) {
        return received;
    }
    memset(buffer, 0, sizeof(*buffer));
    received = msgrcv(request_queue, &buffer->request,
                      sizeof(Request) - sizeof(long) + 1, -highest_message_type,
                      IPC_NOWAIT | MSG_NOERROR);
    if (received >= 0 || (errno != ENOMSG && errno != EINTR)) {
        return received;
    }
    /* A type-zero receive catches any unexpected positive mtype. */
    memset(buffer, 0, sizeof(*buffer));
    return msgrcv(request_queue, &buffer->request,
                  sizeof(Request) - sizeof(long) + 1, 0L,
                  IPC_NOWAIT | MSG_NOERROR);
}

static void handle_worker_exit(WorkerClass *worker, int wait_status)
{
    if (!worker->alive) {
        return;
    }
    worker->alive = 0;
    worker->worker_pid = -1;
    {
        struct msqid_ds queue_info;
        if (msgctl(worker->queue_id, IPC_STAT, &queue_info) < 0 &&
            (errno == EINVAL || errno == EIDRM)) {
            worker->queue_removed = 1;
        }
    }
    fprintf(stderr, "[router] ALERT: %s worker exited (status=%d); class disabled\n",
            worker->name, WIFEXITED(wait_status) ? WEXITSTATUS(wait_status) : -1);
    if (worker->queue_removed) {
        fail_pending_for_worker(worker, STATUS_QUEUE_REMOVED, "QUEUE_REMOVED");
    } else {
        fail_pending_for_worker(worker, STATUS_WORKER_FAILED,
                                "WORKER_FAILED: worker exited unexpectedly");
    }
}

static void reap_workers(void)
{
    int wait_status;
    pid_t exited_pid;
    while ((exited_pid = waitpid(-1, &wait_status, WNOHANG)) > 0) {
        size_t index;
        for (index = 0; index < WORKER_COUNT; index++) {
            if (workers[index].worker_pid == exited_pid) {
                handle_worker_exit(&workers[index], wait_status);
                break;
            }
        }
    }
}

static void worker_process(size_t worker_index)
{
    WorkerClass *worker = &workers[worker_index];
    WorkRequest work;
    pid_t expected_parent = getppid();
    struct sigaction ignore_sigint;
    memset(&ignore_sigint, 0, sizeof(ignore_sigint));
    ignore_sigint.sa_handler = SIG_IGN;
    sigemptyset(&ignore_sigint.sa_mask);
    sigaction(SIGINT, &ignore_sigint, NULL);
    {
        struct sigaction worker_sigterm;
        memset(&worker_sigterm, 0, sizeof(worker_sigterm));
        worker_sigterm.sa_handler = handle_worker_sigterm;
        sigemptyset(&worker_sigterm.sa_mask);
        sigaction(SIGTERM, &worker_sigterm, NULL);
    }
    if (prctl(PR_SET_PDEATHSIG, SIGTERM) < 0) {
        perror("prctl(PR_SET_PDEATHSIG)");
        _exit(1);
    }
    if (getppid() != expected_parent) {
        _exit(1);
    }
    for (;;) {
        ssize_t received = msgrcv(worker->queue_id, &work,
                                  sizeof(work) - sizeof(long), 1L, 0);
        if (received < 0) {
            if (errno == EINTR && !worker_shutdown) {
                continue;
            }
            if (worker_shutdown) {
                break;
            }
            _exit((errno == EIDRM || errno == EINVAL) ? 0 : 1);
        }
        if ((size_t)received != sizeof(work) - sizeof(long) ||
            work.magic != MAGIC || work.class_id != worker->class_id ||
            work.payload_len == 0 || work.payload_len > PAYLOAD_SIZE ||
            work.payload[work.payload_len - 1] != '\0') {
            continue;
        }
        if (strcmp(work.payload, "SLOW") == 0) {
            sleep(2);
        }
        if (worker_shutdown) {
            break;
        }
        {
            WorkResponse response;
            memset(&response, 0, sizeof(response));
            response.mtype = 1L;
            response.magic = MAGIC;
            response.class_id = worker->class_id;
            response.request_id = work.request_id;
            response.status = STATUS_OK;
            snprintf(response.text, sizeof(response.text), "%s worker processed: %.220s",
                     worker->name, work.payload);
            if (msgsnd(response_queue, &response,
                       sizeof(response) - sizeof(long), IPC_NOWAIT) < 0 &&
                (errno == EIDRM || errno == EINVAL)) {
                _exit(0);
            }
        }
    }
    if (worker->queue_id >= 0) {
        msgctl(worker->queue_id, IPC_RMID, NULL);
    }
    if (response_queue >= 0) {
        msgctl(response_queue, IPC_RMID, NULL);
    }
    if (request_queue >= 0) {
        msgctl(request_queue, IPC_RMID, NULL);
    }
    _exit(0);
}

static int start_workers(void)
{
    size_t index;
    for (index = 0; index < WORKER_COUNT; index++) {
        workers[index].queue_id = msgget(IPC_PRIVATE, IPC_CREAT | 0600);
        if (workers[index].queue_id < 0) {
            perror("msgget worker queue");
            return 0;
        }
        {
            struct msqid_ds queue_info;
            if (msgctl(workers[index].queue_id, IPC_STAT, &queue_info) == 0) {
                queue_info.msg_qbytes = 1024;
                if (msgctl(workers[index].queue_id, IPC_SET, &queue_info) < 0) {
                    perror("msgctl IPC_SET worker queue size");
                    return 0;
                }
            }
        }
    }
    for (index = 0; index < WORKER_COUNT; index++) {
        pid_t child = fork();
        if (child < 0) {
            perror("fork worker");
            return 0;
        }
        if (child == 0) {
            worker_process(index);
            _exit(0);
        }
        workers[index].worker_pid = child;
        workers[index].alive = 1;
    }
    return 1;
}

static void cleanup_router(void)
{
    size_t index;
    for (index = 0; index < WORKER_COUNT; index++) {
        if (workers[index].worker_pid > 0) {
            kill(workers[index].worker_pid, SIGTERM);
        }
    }
    for (index = 0; index < WORKER_COUNT; index++) {
        if (workers[index].worker_pid > 0) {
            while (waitpid(workers[index].worker_pid, NULL, 0) < 0 && errno == EINTR) {
            }
            workers[index].worker_pid = -1;
        }
    }
    for (index = 0; index < WORKER_COUNT; index++) {
        if (workers[index].queue_id >= 0) {
            msgctl(workers[index].queue_id, IPC_RMID, NULL);
            workers[index].queue_id = -1;
        }
    }
    if (response_queue >= 0) {
        msgctl(response_queue, IPC_RMID, NULL);
        response_queue = -1;
    }
    if (request_queue >= 0) {
        msgctl(request_queue, IPC_RMID, NULL);
        request_queue = -1;
    }
}

static int router_process(void)
{
    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_handler = handle_signal;
    sigemptyset(&action.sa_mask);
    sigaction(SIGINT, &action, NULL);
    sigaction(SIGTERM, &action, NULL);
    sigaction(SIGUSR1, &action, NULL);
    request_queue = msgget(ROUTER_KEY, IPC_CREAT | IPC_EXCL | 0600);
    if (request_queue < 0) {
        if (errno == EEXIST) {
            fprintf(stderr, "Router queue exists; run './mq_router cleanup' if no router is active.\n");
        } else {
            perror("msgget request queue");
        }
        return 1;
    }
    response_queue = msgget(IPC_PRIVATE, IPC_CREAT | 0600);
    if (response_queue < 0 || !start_workers()) {
        perror("router IPC initialization");
        cleanup_router();
        return 1;
    }
    printf("[router] running pid=%ld request_queue=%d response_queue=%d\n",
           (long)getpid(), request_queue, response_queue);
    {
        size_t index;
        for (index = 0; index < WORKER_COUNT; index++) {
            printf("[router] worker %-8s pid=%ld queue=%d mtype=%ld\n",
                   workers[index].name, (long)workers[index].worker_pid,
                   workers[index].queue_id, workers[index].message_type);
        }
    }
    puts("[router] SIGUSR1 prints statistics; Ctrl+C stops the router.");
    fflush(stdout);
    while (!shutdown_requested) {
        int processed_any = 0;
        RequestBuffer buffer;
        ssize_t received;
        reap_workers();
        if (stats_requested) {
            stats_requested = 0;
            print_stats();
        }
        for (;;) {
            WorkerClass *worker = NULL;
            const char *reason = NULL;
            received = receive_one_request(&buffer);
            if (received < 0) {
                if (errno == ENOMSG || errno == EINTR) {
                    break;
                }
                if (errno == EIDRM || errno == EINVAL) {
                    shutdown_requested = 1;
                    break;
                }
                perror("msgrcv request queue");
                break;
            }
            processed_any = 1;
            if (!validate_request(&buffer.request, received, &worker, &reason)) {
                reject_request(&buffer.request, worker, reason,
                               received >= (ssize_t)(offsetof(Request, payload) - sizeof(long)));
                continue;
            }
            route_request(&buffer.request, worker);
        }
        {
            WorkResponse response;
            ssize_t response_size;
            while ((response_size = msgrcv(response_queue, &response,
                    sizeof(response) - sizeof(long), 1L, IPC_NOWAIT | MSG_NOERROR)) >= 0) {
                if ((size_t)response_size != sizeof(response) - sizeof(long) ||
                    response.magic != MAGIC) {
                    fprintf(stderr, "[router] rejected malformed worker response\n");
                    continue;
                }
                processed_any = 1;
                deliver_response(&response);
            }
            if (response_size < 0 && errno != ENOMSG && errno != EINTR &&
                errno != EIDRM && errno != EINVAL) {
                perror("msgrcv response queue");
            }
        }
        reap_workers();
        if (!processed_any) {
            sleep_milliseconds(2);
        }
    }
    print_stats();
    cleanup_router();
    puts("[router] stopped; owned queues removed.");
    return 0;
}

static unsigned long long make_request_id(void)
{
    unsigned long long value = ((unsigned long long)(unsigned)getpid() << 32) ^
                               (unsigned long long)time(NULL) ^
                               (unsigned long long)clock();
    return value == 0 ? 1 : value;
}

static int client_process(int argument_count, char **arguments)
{
    WorkerClass *worker = NULL;
    Request request;
    ClientResponse response;
    int private_reply_queue;
    int queue;
    size_t payload_length;
    time_t start_time;
    ssize_t received;
    if (argument_count < 4) {
        fprintf(stderr, "Usage: %s client <standard|priority|batch> <message>\n", arguments[0]);
        return 2;
    }
    {
        size_t index;
        for (index = 0; index < WORKER_COUNT; index++) {
            if (strcasecmp(arguments[2], workers[index].name) == 0) {
                worker = worker_for_class(workers[index].class_id);
                break;
            }
        }
    }
    if (worker == NULL) {
        fprintf(stderr, "Unknown client class.\n");
        return 2;
    }
    payload_length = strlen(arguments[3]);
    if (payload_length == 0 || payload_length >= PAYLOAD_SIZE) {
        fprintf(stderr, "Payload must contain 1-%d bytes.\n", PAYLOAD_SIZE - 1);
        return 2;
    }
    queue = msgget(ROUTER_KEY, 0600);
    if (queue < 0) {
        perror("Router unavailable (msgget permissions or queue)");
        return 1;
    }
    private_reply_queue = msgget(IPC_PRIVATE, IPC_CREAT | 0600);
    if (private_reply_queue < 0) {
        perror("msgget private reply queue");
        return 1;
    }
    memset(&request, 0, sizeof(request));
    request.mtype = worker->message_type;
    request.magic = MAGIC;
    request.version = VERSION;
    request.class_id = worker->class_id;
    request.client_pid = getpid();
    request.request_id = make_request_id();
    request.reply_queue = private_reply_queue;
    request.payload_len = (unsigned)payload_length + 1;
    memcpy(request.payload, arguments[3], payload_length + 1);
    if (msgsnd(queue, &request, sizeof(request) - sizeof(long), IPC_NOWAIT) < 0) {
        perror("msgsnd request (queue full or permission denied)");
        msgctl(private_reply_queue, IPC_RMID, NULL);
        return 1;
    }
    printf("[client:%s] sent request_id=%llu\n", worker->name, request.request_id);
    start_time = time(NULL);
    while (time(NULL) - start_time < CLIENT_TIMEOUT) {
        received = msgrcv(private_reply_queue, &response,
                          sizeof(response) - sizeof(long), 1L, IPC_NOWAIT);
        if (received >= 0 && (size_t)received == sizeof(response) - sizeof(long) &&
            response.magic == MAGIC && response.request_id == request.request_id) {
            printf("[client] response id=%llu status=%d: %s\n",
                   response.request_id, response.status, response.text);
            msgctl(private_reply_queue, IPC_RMID, NULL);
            return response.status == STATUS_OK ? 0 : 1;
        }
        if (received < 0 && errno != ENOMSG && errno != EINTR) {
            perror("msgrcv private reply queue");
            break;
        }
        sleep_milliseconds(10);
    }
    fprintf(stderr, "[client] TIMEOUT waiting for request_id=%llu\n", request.request_id);
    msgctl(private_reply_queue, IPC_RMID, NULL);
    return 1;
}

static int send_bad_request(int queue, long message_type, const void *request,
                            size_t message_size, const char *label)
{
    if (msgsnd(queue, request, message_size, IPC_NOWAIT) < 0) {
        fprintf(stderr, "[test] could not submit %s: %s\n", label, strerror(errno));
        return 0;
    }
    printf("[test] submitted malformed case: %s (mtype=%ld)\n", label, message_type);
    return 1;
}

static int test_malformed_requests(void)
{
    int queue = msgget(ROUTER_KEY, 0600);
    int reply_queue;
    Request request;
    size_t base_size = sizeof(Request) - sizeof(long);
    if (queue < 0) {
        perror("Router unavailable");
        return 1;
    }
    reply_queue = msgget(IPC_PRIVATE, IPC_CREAT | 0600);
    if (reply_queue < 0) {
        perror("msgget test reply queue");
        return 1;
    }
    memset(&request, 0, sizeof(request));
    request.mtype = 1L;
    request.magic = MAGIC;
    request.version = VERSION;
    request.class_id = CLASS_STANDARD;
    request.client_pid = getpid();
    request.request_id = make_request_id();
    request.reply_queue = reply_queue;
    request.payload_len = 2;
    memcpy(request.payload, "x", 2);

    request.magic ^= 0x1u;
    send_bad_request(queue, request.mtype, &request, base_size, "wrong magic");
    request.magic = MAGIC;
    request.version++;
    request.request_id++;
    send_bad_request(queue, request.mtype, &request, base_size, "wrong version");
    request.version = VERSION;
    request.class_id = 99;
    request.request_id++;
    send_bad_request(queue, request.mtype, &request, base_size, "unknown class");
    request.class_id = CLASS_STANDARD;
    request.request_id++;
    send_bad_request(queue, request.mtype, &request, base_size - 3, "short message");
    request.request_id++;
    {
        RawMessage oversized;
        memset(&oversized, 0, sizeof(oversized));
        oversized.mtype = request.mtype;
        memcpy(oversized.bytes, (const unsigned char *)&request + sizeof(long), base_size);
        memset(oversized.bytes + base_size, 'Z', 300);
        send_bad_request(queue, request.mtype, &oversized, base_size + 300,
                         "oversized message");
    }
    request.request_id++;
    request.payload[0] = 'x';
    request.payload[1] = 'y';
    request.payload_len = 2;
    send_bad_request(queue, request.mtype, &request, base_size, "missing NUL terminator");
    request.request_id++;
    request.payload[1] = '\0';
    request.mtype = 99L;
    send_bad_request(queue, request.mtype, &request, base_size, "wrong message type");
    {
        unsigned rejected = 0;
        time_t deadline = time(NULL) + 5;
        ClientResponse test_response;
        while (rejected < 7 && time(NULL) <= deadline) {
            ssize_t response_size = msgrcv(reply_queue, &test_response,
                sizeof(test_response) - sizeof(long), 1L, IPC_NOWAIT);
            if (response_size >= 0 &&
                (size_t)response_size == sizeof(test_response) - sizeof(long) &&
                test_response.magic == MAGIC && test_response.status == STATUS_REJECTED) {
                rejected++;
                printf("[test] router rejected malformed case %u/7: %s\n",
                       rejected, test_response.text);
            } else if (response_size < 0 && errno != ENOMSG && errno != EINTR) {
                break;
            } else {
                sleep_milliseconds(10);
            }
        }
        msgctl(reply_queue, IPC_RMID, NULL);
        if (rejected != 7) {
            fprintf(stderr, "[test] expected 7 rejection responses, received %u\n", rejected);
            return 1;
        }
    }
    return 0;
}

static int cleanup_stale_queue(void)
{
    int queue = msgget(ROUTER_KEY, 0600);
    if (queue < 0) {
        if (errno == ENOENT) {
            puts("No router request queue to clean.");
            return 0;
        }
        perror("msgget router queue");
        return 1;
    }
    if (msgctl(queue, IPC_RMID, NULL) < 0) {
        perror("IPC_RMID router queue");
        return 1;
    }
    puts("Removed the well-known router queue. Stop any active router first.");
    return 0;
}

int main(int argument_count, char **arguments)
{
    setvbuf(stdout, NULL, _IOLBF, 0);
    if (argument_count < 2) {
        fprintf(stderr, "Usage: %s router | client <class> <message> | test | cleanup\n",
                arguments[0]);
        return 2;
    }
    if (strcmp(arguments[1], "router") == 0) {
        return router_process();
    }
    if (strcmp(arguments[1], "client") == 0) {
        return client_process(argument_count, arguments);
    }
    if (strcmp(arguments[1], "test") == 0) {
        return test_malformed_requests();
    }
    if (strcmp(arguments[1], "cleanup") == 0) {
        return cleanup_stale_queue();
    }
    fprintf(stderr, "Unknown command: %s\n", arguments[1]);
    return 2;
}