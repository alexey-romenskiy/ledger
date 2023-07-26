#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <liburing.h>
#include <sys/utsname.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <pthread.h>
#include <stdatomic.h>

#include "utils.h"
#include "disruptor.h"
#include "futex.h"

#define MAX_REQUESTS 0x10000

typedef struct req_struct req_t;

struct req_struct {
    req_t *next;
    uint64_t pos;
    uint64_t written;
    struct timespec startTime;
};

void enqueue(req_t **tail, req_t *i) {
    req_t *t = *tail;
    if (t) {
        i->next = t->next;
        t->next = i;
    } else {
        i->next = i;
    }
    *tail = i;
}

req_t *dequeue(req_t **tail) {
    req_t *t = *tail;
    if (t) {
        req_t *n = t->next;
        if (n == t) {
            *tail = NULL;
        } else {
            t->next = n->next;
        }
        return n;
    }
    return NULL;
}


void writeBlock(const void *mapAddr, struct io_uring_sqe *sqe, req_t *req) {
    io_uring_prep_write_fixed(sqe, 0, mapAddr + req->pos, PAGE_SIZE - req->written, req->pos, 0);
    io_uring_sqe_set_data(sqe, req);
    sqe->flags |= IOSQE_FIXED_FILE;
}

void run_test(struct io_uring *ring, void *mapAddr, uint64_t writeSize) {

    struct timespec st;
    startTime(&st);

    req_t *freeHead = NULL;
    req_t reqs[MAX_REQUESTS];
    for (int i = MAX_REQUESTS - 1; i >= 0; i--) {
        reqs[i].next = freeHead;
        freeHead = reqs + i;
    }

    uint64_t endPos = writeSize;
    uint64_t nextPos = 0;
    int pendingRequestCount = 0;
    bool needSubmit = false;
    req_t *retryTail = NULL;

    int64_t min = INT64_MAX;
    int64_t max = -1;
    int64_t sum = 0;
    int64_t count = 0;
    int64_t fullRetries = 0;
    int64_t partRetries = 0;
    int64_t busyTimes = 0;

    while (nextPos < endPos || pendingRequestCount) {

        struct io_uring_cqe *cqe;

        while (pendingRequestCount) {
            int result = io_uring_peek_cqe(ring, &cqe);
            if (result < 0) {
                if (result != -EAGAIN) {
                    REPORT("io_uring_peek_cqe");
                }
                break;
            }
            req_t *req = io_uring_cqe_get_data(cqe);
            int res = cqe->res;
            if (res < 0) {
                if (res != -EAGAIN) {
                    REPORT("io_uring_cqe_get_data");
                }
                fullRetries++;
//                printf("full retry %ld %d\n", count, pendingRequestCount);
                enqueue(&retryTail, req);
            } else {
                req->written += res;
                if (req->written < PAGE_SIZE) {
                    req->pos += res;
                    partRetries++;
//                    printf("part retry %ld %d\n", count, pendingRequestCount);
                    enqueue(&retryTail, req);
                } else {
                    int64_t i = elapsed2(req->startTime);
                    if (min > i) {
                        min = i;
                    }
                    if (max < i) {
                        max = i;
                    }
                    sum += i;
                    count++;
                    pendingRequestCount--;
                    req->next = freeHead;
                    freeHead = req;
//                    printf("written %ld %d\n", count, pendingRequestCount);
                }
            }
            io_uring_cqe_seen(ring, cqe);
        }

        struct io_uring_sqe *sqe;

        while (retryTail && (sqe = io_uring_get_sqe(ring))) {
            req_t *req = dequeue(&retryTail);
            writeBlock(mapAddr, sqe, req);
            needSubmit = true;
        }

        while (nextPos < endPos && freeHead && (sqe = io_uring_get_sqe(ring))) {
            req_t *req = freeHead;
            freeHead = freeHead->next;
            req->pos = nextPos;
            req->written = 0;
            startTime(&req->startTime);
            nextPos += PAGE_SIZE;
            pendingRequestCount++;
            writeBlock(mapAddr, sqe, req);
            needSubmit = true;
        }

        if (needSubmit) {
            int r = io_uring_submit(ring);
            if (r < 0) {
                if (r != -EBUSY) {
                    REPORT("io_uring_submit");
                }
                busyTimes++;
            } else {
                needSubmit = false;
            }
        }
    }


//    io_uring_prep_send_zc_fixed(sqe, outfd, mapAddr + PAGE_SIZE, PAGE_SIZE, 0, 0, 0);
//    ERROR("io_uring_wait_cqe", io_uring_wait_cqe(&ring, &cqe));

    int64_t total = elapsed2(st);

    printf("Ok min=%ld avg=%ld max=%ld fullRetries=%ld partRetries=%ld busyTimes=%ld total=%ld\n", min, sum / count,
           max, fullRetries, partRetries, busyTimes, total);
}

void randomize(void *mapAddr, uint64_t writeSize) {

    struct timespec t;
    startTime(&t);

//    printf("Randomizing\n");
    srandom(time(NULL));
    uint16_t *p = mapAddr;
    void *endAddr = mapAddr + writeSize;
    while ((void *) p < endAddr) {
        *p++ = random() >> 8;
    }
//    printf("Randomized\n");

//    printf("delta %ld\n", elapsed(&t));

    sleep(1);

    uint64_t end = writeSize / PAGE_SIZE / 2 * PAGE_SIZE;

    for (int i = 0; i < 5; i++) {
        startTime(&t);
        memcpy(mapAddr + end, mapAddr, end);
        printf("cpy %ld\n", elapsed2(t) / (end / PAGE_SIZE));
    }
}

long counter = 0;
pid_t lastPid;

pthread_mutex_t mutex;
pthread_cond_t cond;

sigset_t set;

void action(int sig, siginfo_t *info, void *ucontext) {


    if (info->si_code == SI_USER) {
        long i = atomic_load_explicit(&counter, memory_order_acquire);
        long n = i + 1;
        lastPid = info->si_pid;
        while (!atomic_compare_exchange_strong(&counter, &i, n));
        sigaddset(&((ucontext_t *) ucontext)->uc_sigmask, SIGUSR1);
//        atomic_store_explicit(&counter, n, memory_order_release);
//        pthread_mutex_lock(&mutex);
//        pthread_cond_broadcast(&cond);
//        pthread_mutex_unlock(&mutex);
    } else if (info->si_code == SI_QUEUE) {
        uint64_t value = (uint64_t) info->si_value.sival_ptr;
    }
}

void action2(int sig, siginfo_t *info, void *ucontext) {

    if (info->si_code == SI_USER) {
        pthread_mutex_lock(&mutex);
        pthread_cond_broadcast(&cond);
        pthread_mutex_unlock(&mutex);
    }
}

//#include <sys/types.h>
//#include <sys/socket.h>
//#include <sys/un.h>


int main(int argc, char *argv[]) {

    if (true) {
        printf("pid: %d\n", getpid());

        pthread_mutex_init(&mutex, NULL);
        pthread_cond_init(&cond, NULL);

        struct sigaction act;
        memset(&act, 0, sizeof(act));
        act.sa_flags = SA_SIGINFO;
        act.sa_sigaction = &action2;
        sigaction(SIGUSR1, &act, NULL);

        int lock = pthread_mutex_lock(&mutex);
        printf("Locked %d\n", lock);
        int wait = pthread_cond_wait(&cond, &mutex);
        printf("Unblock %d\n", wait);
        pthread_mutex_unlock(&mutex);
        // Do not unblock signal when mutex is acquired!
        // The pending signal will cause deadlock if the handler will try to acquire this mutex!
        return 0;
    }

    if (false) {
        /*struct sockaddr_un client_sockaddr;
        memset(&client_sockaddr, 0, sizeof(client_sockaddr));

        socklen_t len = sizeof(client_sockaddr);
        getpeername(0, &client_sockaddr, &len);*/

        printf("pid: %d\n", getpid());

        struct sigaction act;
        memset(&act, 0, sizeof(act));
        act.sa_flags = SA_SIGINFO;
        act.sa_sigaction = &action;
        sigaction(SIGUSR1, &act, NULL);

        long i = atomic_load_explicit(&counter, memory_order_acquire);

        sigemptyset(&set);
        sigaddset(&set, SIGUSR1);

//    sigset_t set2;
//    sigprocmask(SIG_SETMASK, NULL, &set2);

//    pthread_sigmask(SIG_BLOCK, &set, NULL);
//    sigprocmask(SIG_BLOCK, &set, NULL);

        pthread_mutexattr_t attr;
        pthread_mutexattr_init(&attr);
//    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
//    pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
        pthread_mutex_init(&mutex, &attr);
        pthread_mutexattr_destroy(&attr);

        pthread_condattr_t attr2;
        pthread_condattr_init(&attr2);
//    pthread_condattr_setpshared(&attr2, PTHREAD_PROCESS_SHARED);
        pthread_cond_init(&cond, &attr2);
        pthread_condattr_destroy(&attr2);

        /*printf("Lock\n");
        int lock = pthread_mutex_lock(&mutex);
        printf("Wait %d\n", lock);
        int wait = pthread_cond_wait(&cond, &mutex);
        printf("Unblock %d\n", wait);
        pthread_mutex_unlock(&mutex);*/

        while (true) {
            while (true) {
//        int sig;
//        int r = sigwait(&set, &sig);
//        printf("sigwait r=%d sig=%d\n", r, sig);
                long n = atomic_load_explicit(&counter, memory_order_acquire);
                if (n != i) {
                    i = n;
                    printf("Increment: %ld pid: %d\n", n, lastPid);
                    break;
                }
            }

            printf("Sleep\n");
            sleep(10);
            printf("Resume\n");

//        sigprocmask(SIG_SETMASK, &set2, NULL);
            sigset_t set2;
            sigemptyset(&set2);
            sigaddset(&set2, SIGUSR1);

            pthread_sigmask(SIG_UNBLOCK, &set2, NULL);
        }


        if (false) {
            pid_t pid = getpid();
            kill(pid, SIGUSR1); // 0 or -1 is returned and errno
            /* EINVAL An invalid signal was specified.
               EPERM  The calling process does not have permission to send the signal to any of the target processes.
               ESRCH  The target process or process group does not exist.  Note that an existing process might be a zombie, a process that has terminated execution, but has not yet been wait(2)ed for.
               */

            union sigval sv;
            sv.sival_ptr = (void *) 123;
            sigqueue(pid, SIGRTMIN, sv); // 0 or -1 is returned and errno
            /* EAGAIN The limit of signals which may be queued has been reached.  (See signal(7) for further information.)
               EINVAL sig was invalid.
               EPERM  The process does not have permission to send the signal to the receiving process.  For the required permissions, see kill(2).
               ESRCH  No process has a PID matching pid.
               */
        }

        /*pthread_sigmask()*/
    }

    uint64_t writeSize = HUGE_PAGE_SIZE;
//    uint64_t writeSize = PAGE_SIZE;

    pid_t tid = gettid();

    printf("tid=%d\n", tid);

    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(8, &cpu_set);

    ERROR("sched_setaffinity", sched_setaffinity(tid, sizeof(cpu_set_t), &cpu_set));

    printf("count=%d\n", CPU_COUNT(&cpu_set));

    struct rlimit rl;

    ERROR("getrlimit", getrlimit(RLIMIT_MEMLOCK, &rl));

    printf("limit soft=%lu hard=%lu\n", rl.rlim_cur, rl.rlim_max);

    int outfd = open("/mnt/data/home/devtest/out.txt", O_RDWR | O_CREAT | O_DIRECT | O_DSYNC, 0644);
//    int outfd = open("/home/devtest/out.txt", O_RDWR | O_CREAT | O_DIRECT | O_DSYNC, 0644);
    ERROR("open out.txt", outfd);

    int mapfd = open("/run/devtest/map.txt", O_RDWR | O_CREAT, 0644);
    ERROR("open map.txt", mapfd);

    ERROR("truncate", ftruncate(mapfd, HUGE_PAGE_SIZE));

    void *mapAddr = mmap(NULL, HUGE_PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED_VALIDATE | MAP_HUGE_PAGE, mapfd, 0);
    if (!mapAddr) {
        REPORT("map file");
    }

    close(mapfd);

    struct iovec iov[1];

    iov[0].iov_base = mapAddr;
    iov[0].iov_len = HUGE_PAGE_SIZE;

    struct io_uring_params params;
    memset(&params, 0, sizeof(params));
    params.flags |= IORING_SETUP_IOPOLL | IORING_SETUP_SQPOLL | IORING_SETUP_SQ_AFF;
    params.sq_thread_idle = INT32_MAX;
    params.sq_thread_cpu = 9;

    struct io_uring ring;
    ERROR2("io_uring_queue_init_params", io_uring_queue_init_params(1 << 8, &ring, &params));

    printf("ring.features=%08x\n", ring.features);

//    ERROR("setup uring", io_uring_queue_init(1 << 8, &ring, 0));

    ERROR("io_uring_register_ring_fd", io_uring_register_ring_fd(&ring));

    ERROR("io_uring_register_files", io_uring_register_files(&ring, &outfd, 1));

    ERROR2("registering buffers", io_uring_register_buffers(&ring, iov, countof(iov)));

/*
    startTime(&st);
    while (elapsed2(st) < 5000000000) {

    }

    startTime(&st);
    ssize_t i2 = write(outfd, mapAddr, PAGE_SIZE);
    int64_t i1 = elapsed2(st);
    printf("time %ld %ld\n", i1, i2);
*/

//    ERROR("sched_setaffinity", sched_setaffinity(tid, sizeof(cpu_set_t), &cpu_set));

//    io_uring_enter(ring.ring_fd, 0, 0, 0, NULL);
//    io_uring_submit(&ring); // warm up

    for (int i = 0; i < 1; i++) {
        randomize(mapAddr, writeSize);

        struct timespec st;
        startTime(&st);
        while (elapsed2(st) < 2000000000) {

        }

        run_test(&ring, mapAddr, writeSize);
    }

    close(outfd);

    io_uring_queue_exit(&ring);
}
