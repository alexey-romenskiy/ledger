//
// Created by alexe on 23.07.2023.
//

#ifndef BROKER_DISRUPTOR_H
#define BROKER_DISRUPTOR_H

#include <stdint.h>
#include <stdbool.h>
#include <sys/user.h>
#include <pthread.h>

#define cache_align __attribute__((aligned(64)))
#define page_align __attribute__((aligned(PAGE_SIZE)))

typedef struct {
    cache_align uint64_t head; // UINT64_MAX if stack is empty
} Bloom;

typedef struct {
    uint64_t sequence;
    uint64_t offset;
} Position;

typedef struct {
    cache_align uint64_t readySize; // updated by producer
    Position readyPosition; // updated by consumer
    pthread_mutex_t readyPositionMutex;
    pthread_cond_t readyPositionCond; // signaled by producer, waited by consumer & subscribers

    uint64_t writingSize; // local for consumer
    Position writingPosition; // local for consumer

    uint64_t stableSize; // updated by consumer
    Position stablePosition; // updated by consumer
    pthread_mutex_t stablePositionMutex;
    pthread_cond_t stablePositionCond; // signaled by consumer, waited by subscribers

    uint64_t topicId; // immutable
    uint64_t memoryOffset; // immutable
    uint64_t virtBlockNumber; // local for consumer
    uint64_t fileId; // local for consumer
    uint64_t fileBlockNumber; // local for consumer

    uint64_t refCount; // local for consumer

    bool writing; // local for consumer
    bool cached; // local for consumer
    bool readFailed; // updated by consumer
    bool writeFailed; // updated by consumer

    cache_align bool readyPositionLock; // producer/consumer spinlock for readySize & readyPosition
    cache_align bool stablePositionLock; // producer/consumer spinlock for stableSize & stablePosition
} PrivateBlockBuffer;

typedef struct {
    cache_align uint64_t readySize; // updated by producer
    uint64_t memoryOffset; // immutable
} ProducerBlockBuffer;

typedef struct {
    uint64_t blockSize; // immutable
    uint64_t blockMask; // immutable
    uint64_t ringBufferSize; // immutable
    uint64_t ringBufferMask; // immutable
    page_align PrivateBlockBuffer *entries[]; // updated by consumer
} ConsumerTopicInfo;

typedef struct {
    pthread_mutex_t producerMutex;
    pthread_cond_t producerCond; // signaled by consumer when ringBufferHead changes & producerNotify = true, waited by producer
    cache_align bool producerNotify; // whether to signal producerCond
    uint64_t thisBee; // immutable
    uint64_t blockSize; // immutable
    uint64_t blockMask; // immutable
    uint64_t ringBufferSize; // immutable
    uint64_t ringBufferMask; // immutable
    cache_align uint64_t nextBee; // reference to the next element in the ready bees stack, zero if not queued, UINT64_MAX for stack bottom
    cache_align uint64_t ringBufferTail; // updated by producer
    cache_align uint64_t ringBufferHead; // updated by consumer
    page_align uint64_t entries[]; // updated by consumer, offsets to ProducerBlockBuffer
} ProducerTopicInfo;

typedef struct {
    pthread_mutex_t consumerMutex;
    pthread_cond_t consumerCond; // signaled by producer when a bee available & consumerNotify = true, waited by consumer
    cache_align bool consumerNotify; // whether to signal consumerCond
    cache_align Bloom bloom;
} ProducerBrokerInfo;

#endif //BROKER_DISRUPTOR_H
