#include "utils.h"
#include <stdio.h>
#include <string.h>
#include <errno.h>

unsigned clz64(uint64_t x) {
    if (x == 0) return 64;
    int n = 0;
    if ((x & 0xFFFFFFFF00000000) == 0) {
        n += 32;
        x <<= 32;
    }
    if ((x & 0xFFFF000000000000) == 0) {
        n += 16;
        x <<= 16;
    }
    if ((x & 0xFF00000000000000) == 0) {
        n += 8;
        x <<= 8;
    }
    if ((x & 0xF000000000000000) == 0) {
        n += 4;
        x <<= 4;
    }
    if ((x & 0xC000000000000000) == 0) {
        n += 2;
        x <<= 2;
    }
    if ((x & 0x8000000000000000) == 0) { n++; }
    return n;
}

uint64_t rshift(uint64_t v, unsigned n) {
    if (n < 64) {
        return v >> n;
    }
    return 0;
}

uint64_t binaryCeil(uint64_t v) {
    uint64_t x = rshift(UINT64_MAX, clz64(v) + 1);
    return (v + x) & ~x;
}

bool error(const char *op, int result) {

    if (result < 0) {
        report(op);
        return true;
    } else {
        return false;
    }
}

void report(const char *op) {
    fprintf(stderr, "%s failed: %s\n", op, strerror(errno));
}

bool error2(const char *op, int result) {

    if (result < 0) {
        report2(op, result);
        return true;
    } else {
        return false;
    }
}

void report2(const char *op, int result) {
    fprintf(stderr, "%s failed: %s\n", op, strerror(-result));
}
