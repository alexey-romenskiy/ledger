package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

public class BlockDeviceWriter2 implements BlockWriter, AutoCloseable {

    private final AtomicInteger runningBlockIndex = new AtomicInteger();

    private final AtomicInteger runningBlockCount = new AtomicInteger();

    private final AtomicInteger blocksBehind = new AtomicInteger();

    private final int blockSize;

    private final long fileBlockLength;

    private final int maxCached;

    private final int maxBehind;

    private final int maxRunning;

    @Nonnull
    private final AsynchronousFileChannel channel;

    private final BufferPool pool;

    private final AtomicLong freeBlockCount;

    private final QueueItem[] queue;

    private final CompletionHandler<Integer, QueueItem> fullHandler = new CompletionHandler<>() {
        @Override
        public void completed(@Nonnull Integer result, @Nonnull QueueItem attachment) {
            // TODO:
        }

        @Override
        public void failed(@Nonnull Throwable e, @Nonnull QueueItem attachment) {
            // TODO:
        }
    };

    private final CompletionHandler<Integer, QueueItem> partialHandler = new CompletionHandler<>() {
        @Override
        public void completed(@Nonnull Integer result, @Nonnull QueueItem attachment) {
            // TODO:
        }

        @Override
        public void failed(@Nonnull Throwable e, @Nonnull QueueItem attachment) {
            // TODO:
        }
    };

    private long nextWriteBlockOffset;

    private QueueItem currentQueueItem;

    private int currentWriteBlockIndex;

    private boolean currentPartial;

    public BlockDeviceWriter2(int blockSize, long fileBlockLength, int buffersPoolSize, int maxCached, int maxBehind,
            int maxRunning, @Nonnull AsynchronousFileChannel channel) {

        if (blockSize < 1) {
            throw new IllegalArgumentException();
        }

        if (fileBlockLength < maxCached) {
            throw new IllegalArgumentException();
        }

        if (buffersPoolSize < maxCached) {
            throw new IllegalArgumentException();
        }

        if (maxCached < maxBehind) {
            throw new IllegalArgumentException();
        }

        if (maxBehind < maxRunning) {
            throw new IllegalArgumentException();
        }

        if (maxRunning < 1) {
            throw new IllegalArgumentException();
        }

        requireNonNull(channel);

        this.blockSize = blockSize;
        this.fileBlockLength = fileBlockLength;
        this.maxCached = maxCached;
        this.maxBehind = maxBehind;
        this.maxRunning = maxRunning;
        this.channel = channel;
        this.pool = new BufferPool(blockSize, buffersPoolSize);
        this.freeBlockCount = new AtomicLong(fileBlockLength);
        this.queue = new QueueItem[maxCached];
    }

    @Nonnull
    @Override
    public ByteBuffer fullBlock(long sequence, long offset, boolean pending) throws InterruptedException {

        if (currentPartial) {
            currentPartial = false;
            trySendCurrentFull();
        } else {
            trySendNextFull();
        }

        final var queueItem = pool.borrow();
        currentQueueItem = queueItem;
        return queueItem.byteBuffer;
    }

    @Override
    public void partialBlock(long sequence, long offset, boolean pending, int end) throws InterruptedException {

        if (currentPartial) {
            trySendCurrentPartial(end);
        } else {
            currentPartial = true;
            trySendNextPartial(end);
        }
    }

    @Override
    public void close() throws Exception {
        // TODO:
    }

    private void completePartial(@Nonnull QueueItem queueItem) {

    }

    private void trySendCurrentPartial(int end) throws InterruptedException {

        final var source = currentQueueItem.duplicateByteBuffer;
        source.limit(end);

        var queueItem = queue[currentWriteBlockIndex];
        QueueItem item = null;
        while (true) {
            var item2 = queueItem.running.compareAndExchange(item, queueItem);
            if (item == item2) {
                if (item == null) {
                    break;
                } else {

                }
            } else {
                item = item2;
            }
        }

        if (null == queueItem2) {
            source.position(0);
            final var item = pool.borrow();
            item.index = queueItem.index;
            item.fileBlockOffset = queueItem.fileBlockOffset;
            item.length = end;
            item.byteBuffer.put(source);
            queueItem.item.set(item);
            if (queueItem.running.compareAndExchange(true)) {

            } else {

            }
            // TODO:
            return;
        }

        queueItem.length = end;
        queueItem.byteBuffer.put(source);

        doWritePartial(queueItem);
    }

    private void trySendCurrentFull() {

        final var itemIndex = currentWriteBlockIndex;
        currentWriteBlockIndex = (itemIndex + 1) % maxCached;

        final var queueItem = currentQueueItem;
        if (queueItem.running.getAndSet(true)) {
            return;
        }

        doWriteFull(queueItem);
    }

    private void trySendNextPartial(int end) throws InterruptedException {

        final var source = currentQueueItem.duplicateByteBuffer;
        source.limit(end);
        source.position(0);
        final var queueItem = pool.borrow();
        queueItem.length = end;
        queueItem.byteBuffer.put(source);

        final var itemIndex = currentWriteBlockIndex;
        queueItem.index = itemIndex;
        queue[itemIndex] = queueItem;

        final var next = nextWriteBlockOffset;
        queueItem.fileBlockOffset = next;
        nextWriteBlockOffset = (next + 1) % fileBlockLength;

        var freeBlockCount = this.freeBlockCount.get();
        if (freeBlockCount > 0) {
            var runningBlockCount = this.runningBlockCount.get();
            if (runningBlockCount < maxRunning) {
                var blocksBehind = this.blocksBehind.get();
                if (blocksBehind < maxBehind) {
                    while (true) {
                        final var freeBlockCount2 =
                                this.freeBlockCount.compareAndExchange(freeBlockCount, freeBlockCount - 1);
                        if (freeBlockCount == freeBlockCount2) {
                            while (true) {
                                final var runningBlockCount2 = this.runningBlockCount
                                        .compareAndExchange(runningBlockCount, runningBlockCount + 1);
                                if (runningBlockCount == runningBlockCount2) {
                                    while (true) {
                                        final var blocksBehind2 =
                                                this.blocksBehind.compareAndExchange(blocksBehind, blocksBehind + 1);
                                        if (blocksBehind == blocksBehind2) {
                                            if (blocksBehind == 0) {
                                                runningBlockIndex.set(itemIndex);
                                            }
                                            queueItem.running.set(queueItem);
                                            doWritePartial(queueItem);
                                            return;
                                        } else {
                                            if (blocksBehind2 >= maxBehind) {
                                                this.freeBlockCount.incrementAndGet();
                                                break;
                                            }
                                            blocksBehind = blocksBehind2;
                                        }
                                    }
                                    this.runningBlockCount.decrementAndGet();
                                    break;
                                } else {
                                    if (runningBlockCount2 >= maxRunning) {
                                        break;
                                    }
                                    runningBlockCount = runningBlockCount2;
                                }
                            }
                            this.freeBlockCount.incrementAndGet();
                            break;
                        } else {
                            if (freeBlockCount2 <= 0) {
                                break;
                            }
                            freeBlockCount = freeBlockCount2;
                        }
                    }
                }
            }
        }
    }

    private void trySendNextFull() {

        final var queueItem = currentQueueItem;
        final var itemIndex = currentWriteBlockIndex;
        queueItem.index = itemIndex;
        queue[itemIndex] = queueItem;

        final var next = nextWriteBlockOffset;
        queueItem.fileBlockOffset = next;
        nextWriteBlockOffset = (next + 1) % fileBlockLength;

        currentWriteBlockIndex = (itemIndex + 1) % maxCached;

        var freeBlockCount = this.freeBlockCount.get();
        if (freeBlockCount > 0) {
            var runningBlockCount = this.runningBlockCount.get();
            if (runningBlockCount < maxRunning) {
                var blocksBehind = this.blocksBehind.get();
                if (blocksBehind < maxBehind) {
                    while (true) {
                        final var freeBlockCount2 =
                                this.freeBlockCount.compareAndExchange(freeBlockCount, freeBlockCount - 1);
                        if (freeBlockCount == freeBlockCount2) {
                            while (true) {
                                final var runningBlockCount2 = this.runningBlockCount
                                        .compareAndExchange(runningBlockCount, runningBlockCount + 1);
                                if (runningBlockCount == runningBlockCount2) {
                                    while (true) {
                                        final var blocksBehind2 =
                                                this.blocksBehind.compareAndExchange(blocksBehind, blocksBehind + 1);
                                        if (blocksBehind == blocksBehind2) {
                                            if (blocksBehind == 0) {
                                                runningBlockIndex.set(itemIndex);
                                            }
                                            doWriteFull(queueItem);
                                            return;
                                        } else {
                                            if (blocksBehind2 >= maxBehind) {
                                                this.freeBlockCount.incrementAndGet();
                                                break;
                                            }
                                            blocksBehind = blocksBehind2;
                                        }
                                    }
                                    this.runningBlockCount.decrementAndGet();
                                    break;
                                } else {
                                    if (runningBlockCount2 >= maxRunning) {
                                        break;
                                    }
                                    runningBlockCount = runningBlockCount2;
                                }
                            }
                            this.freeBlockCount.incrementAndGet();
                            break;
                        } else {
                            if (freeBlockCount2 <= 0) {
                                break;
                            }
                            freeBlockCount = freeBlockCount2;
                        }
                    }
                }
            }
        }
    }

    private void doWritePartial(@Nonnull QueueItem queueItem) {

        channel.write(queueItem.byteBuffer, queueItem.fileBlockOffset * blockSize, queueItem, partialHandler);
    }

    private void doWriteFull(@Nonnull QueueItem queueItem) {

        channel.write(queueItem.byteBuffer, queueItem.fileBlockOffset * blockSize, queueItem, fullHandler);
    }
}
