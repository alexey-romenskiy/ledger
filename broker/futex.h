#ifndef BROKER_FUTEX_H
#define BROKER_FUTEX_H

#include <linux/futex.h>      /* Definition of FUTEX_* constants */
#include <sys/syscall.h>      /* Definition of SYS_* constants */
#include <unistd.h>
#include <stdint.h>
#include <stdatomic.h>
#include <time.h>

static inline long futex(
        int32_t *uaddr,
        int futex_op,
        int32_t val,
        const struct timespec *timeout,
        int32_t *uaddr2,
        int32_t val3
) {
    return syscall(SYS_futex, uaddr, futex_op, val, timeout, uaddr2, val3);
}

/**
 * @return the number of waiters that were woken up
 */
static inline long futexWake(int32_t *uaddr, int32_t maxWakeupCount) {
    return futex(uaddr, FUTEX_WAKE, maxWakeupCount, NULL, NULL, 0);
}

static inline int32_t futexWait(int32_t *uaddr, int32_t expectedValue, const struct timespec *timeout) {

    futex(uaddr, FUTEX_WAIT, expectedValue, timeout, NULL, 0);
    // reult = 0 - wakeup
    // reult = 1
    //  errno=EINTR - interrupted by signal
    //  errno=EAGAIN - value already changed
    return atomic_load_explicit(uaddr, memory_order_acquire);
}

static inline int32_t optimisticFutexWait(int32_t *uaddr, int32_t expectedValue, const struct timespec *timeout) {

    int32_t witnessValue = atomic_load_explicit(uaddr, memory_order_acquire);
    if (witnessValue != expectedValue) {
        return witnessValue;
    }

    return futexWait(uaddr, expectedValue, timeout);
}

#endif //BROKER_FUTEX_H
