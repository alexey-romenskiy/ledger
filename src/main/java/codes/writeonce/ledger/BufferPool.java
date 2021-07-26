package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BufferPool implements Pool<QueueItem> {

    private final Lock freeLock = new ReentrantLock();

    private final Condition freeCondition = freeLock.newCondition();

    private final AtomicInteger freeHead = new AtomicInteger();

    private final AtomicInteger freeTail = new AtomicInteger();

    private final int poolSize;

    private final QueueItem[] pool;

    private final AtomicInteger freeCount;

    public BufferPool(int blockSize, int buffersPoolSize) {

        poolSize = buffersPoolSize;
        pool = new QueueItem[buffersPoolSize];
        freeCount = new AtomicInteger();

        final var wholeBuffer = ByteBuffer.allocateDirect(blockSize * (buffersPoolSize + 1) - 1);
        final var addressModulus = wholeBuffer.alignmentOffset(0, blockSize);
        final var alignedPosition = addressModulus > 0 ? blockSize - addressModulus : 0;

        for (int i = 0; i < buffersPoolSize; i++) {
            pool[i] = new QueueItem(i, wholeBuffer.slice(alignedPosition + i * blockSize, blockSize));
        }
    }

    @Nonnull
    public QueueItem borrow() throws InterruptedException {

        take();

        while (true) {
            final var current = freeHead.get();
            if (freeHead.compareAndSet(current, (current + 1) % poolSize)) {
                return pool[current];
            }
        }
    }

    public void reclaim(@Nonnull QueueItem queueItem) {

        while (true) {
            final var current = freeTail.get();
            if (freeTail.compareAndSet(current, (current + 1) % poolSize)) {
                queueItem.index = current;
                pool[current] = queueItem;
                break;
            }
        }

        free();
    }

    private void take() throws InterruptedException {

        while (true) {
            var count = freeCount.get();
            if (count == 0) {
                freeLock.lock();
                try {
                    while (true) {
                        count = freeCount.get();
                        if (count != 0) {
                            break;
                        }
                        freeCondition.await();
                    }
                } finally {
                    freeLock.unlock();
                }
            }
            if (freeCount.compareAndSet(count, count - 1)) {
                return;
            }
        }
    }

    private void free() {

        if (freeCount.getAndIncrement() == 0) {
            freeLock.lock();
            try {
                freeCondition.signal();
            } finally {
                freeLock.unlock();
            }
        }
    }
}
