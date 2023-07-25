#ifndef BROKER_UTILS_H
#define BROKER_UTILS_H

#include <stdint.h>
#include <stdbool.h>
#include <sys/mman.h>
#include <sys/user.h>
#include <time.h>
#include <stdlib.h>

#define HUGE_PAGE_2MB_SHIFT        21
#define HUGE_PAGE_1GB_SHIFT        30

#define HUGE_PAGE_SHIFT        HUGE_PAGE_1GB_SHIFT

#define MAP_HUGE_PAGE        (HUGE_PAGE_SHIFT << MAP_HUGE_SHIFT)

#define HUGE_PAGE_SIZE        (1UL << HUGE_PAGE_SHIFT)
#define HUGE_PAGE_MASK        (~(HUGE_PAGE_SHIFT-1))

#define HUGE_PAGE_2MB_SIZE        (1UL << HUGE_PAGE_2MB_SHIFT)
#define HUGE_PAGE_2MB_MASK        (~(HUGE_PAGE_2MB_SHIFT-1))

#define HUGE_PAGE_1GB_SIZE        (1UL << HUGE_PAGE_1GB_SHIFT)
#define HUGE_PAGE_1GB_MASK        (~(HUGE_PAGE_1GB_SHIFT-1))

#define countof(array) (sizeof(array)/sizeof(*array))

unsigned clz64(uint64_t x);

uint64_t rshift(uint64_t v, unsigned n);

uint64_t binaryCeil(uint64_t v);

bool error(const char *op, int result);

#define ERROR(op, result) { if (error(op, result)) exit(-1); }

void report(const char *op);

#define REPORT(op) { report(op); exit(-1); }

bool error2(const char *op, int result);

#define ERROR2(op, result) { if (error2(op, result)) exit(-1); }

void report2(const char *op, int result);

#define REPORT2(op) { report2(op); exit(-1); }

__attribute__((__gnu_inline__, __always_inline__))
static __inline__ void startTime(struct timespec *t) {

    clock_gettime(CLOCK_MONOTONIC_RAW, t);
}

__attribute__((__gnu_inline__, __always_inline__))
static __inline__ int64_t elapsed(struct timespec *t) {

    struct timespec t2;
    clock_gettime(CLOCK_MONOTONIC_RAW, &t2);
    int64_t delta = (t2.tv_sec - t->tv_sec) * 1000000000 + t2.tv_nsec - t->tv_nsec;
    *t = t2;
    return delta;
}

__attribute__((__gnu_inline__, __always_inline__))
static __inline__ int64_t elapsed2(struct timespec t) {

    struct timespec t2;
    clock_gettime(CLOCK_MONOTONIC_RAW, &t2);
    int64_t delta = (t2.tv_sec - t.tv_sec) * 1000000000 + t2.tv_nsec - t.tv_nsec;
    return delta;
}

#endif //BROKER_UTILS_H
