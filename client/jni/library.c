#include "library.h"

#include <stdio.h>
#include <pthread.h>

#include "codes_writeonce_ledger_Lib.h"

JNIEXPORT jint JNICALL Java_codes_writeonce_ledger_Lib_pthread_1mutex_1lock(JNIEnv *, jclass, jlong mutexAddr) {
    return pthread_mutex_lock((pthread_mutex_t *) mutexAddr);
}

JNIEXPORT jint JNICALL Java_codes_writeonce_ledger_Lib_pthread_1mutex_1unlock(JNIEnv *, jclass, jlong mutexAddr) {
    return pthread_mutex_unlock((pthread_mutex_t *) mutexAddr);
}

JNIEXPORT jint JNICALL
Java_codes_writeonce_ledger_Lib_pthread_1cond_1wait(JNIEnv *, jclass, jlong condAddr, jlong mutexAddr) {
    return pthread_cond_wait((pthread_cond_t *) condAddr, (pthread_mutex_t *) mutexAddr);
}

JNIEXPORT jint JNICALL Java_codes_writeonce_ledger_Lib_pthread_1cond_1signal(JNIEnv *, jclass, jlong condAddr) {
    return pthread_cond_signal((pthread_cond_t *) condAddr);
}
