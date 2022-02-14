package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static codes.writeonce.ledger.MessageWriter.MAX_CHUNK_DATA_SIZE;

public class Ledger implements BlockWriter {

    @Nonnull
    private final Pool<BlockBuffer> blockBufferPool;

    private final int blockSize;

    private final int maxCached;

    @Nonnull
    private final AsynchronousFileChannel channel;

    private final QueueItem[] queue;

    @Nonnull
    private final MessageWriter messageWriter;

    @Nonnull
    final Index index;

    @Nullable
    Slot slotHead;

    @Nullable
    Slot slotMin;

    private int currentPosition;

    private int queueUsed;

    long firstNeededBlockFilePosition; // -1 if no such block

    private Thread neededBlockWaiter;

    private final long blockFileSize;

    long blockFilePosition;

    private int partialSize;

    long readyMessageSequence;

    long readyMessageOffset;

    private boolean readyMessageFinished;

    private long stableMessageSequence;

    private long stableMessageOffset;

    private boolean stableMessageFinished;

    final AtomicBoolean queueLock = new AtomicBoolean();

    public final WaiterList readerWaiters = new WaiterList();

    public final WaiterList stabilityWaiters = new WaiterList();

    private Thread diskWaiter;

    private QueueItem headWriting;

    private QueueItem tailWriting;

    @Nonnull
    private final CompletionHandler<Integer, QueueItem> handler = new CompletionHandler<Integer, QueueItem>() {
        @Override
        public void completed(Integer result, QueueItem item) {

            acquire(queueLock);
            try {
                final var prev = item.prevWriting;
                final var size = item.writingSize;
                final var messageSequence = item.writingMessageSequence;
                final var messageOffset = item.writingMessageOffset;
                final var messageFinished = item.writingMessageFinished;

                item.stableSize = size;
                item.stableMessageSequence = messageSequence;
                item.stableMessageOffset = messageOffset;
                item.stableMessageFinished = messageFinished;

                if (size == blockSize) {
                    item.writing = false;

                    final var next = item.nextWriting;

                    final QueueItem head;
                    if (prev == null) {
                        headWriting = next;
                        head = next;
                    } else {
                        head = headWriting;
                        prev.nextWriting = next;
                        item.prevWriting = null;
                    }

                    if (next == null) {
                        tailWriting = prev;
                    } else {
                        next.prevWriting = prev;
                        item.nextWriting = null;
                    }

                    if (head == null) {
                        stableMessageSequence = readyMessageSequence;
                        stableMessageOffset = readyMessageOffset;
                        stableMessageFinished = readyMessageFinished;
                    } else {
                        stableMessageSequence = head.stableMessageSequence;
                        stableMessageOffset = head.stableMessageOffset;
                        stableMessageFinished = head.stableMessageFinished;
                    }

                    final var thread = diskWaiter;
                    if (thread != null) {
                        LockSupport.unpark(thread);
                    }
                } else {
                    if (prev == null) {
                        stableMessageSequence = messageSequence;
                        stableMessageOffset = messageOffset;
                        stableMessageFinished = messageFinished;
                    }

                    if (item.pendingSize > item.stableSize) {
                        tryWrite(item);
                    } else {
                        item.writing = false;
                    }
                }

                stabilityWaiters.wakeup();
            } finally {
                release(queueLock);
            }
        }

        @Override
        public void failed(Throwable throwable, QueueItem item) {
            // TODO:
        }
    };

    public Ledger(
            @Nonnull Pool<BlockBuffer> blockBufferPool,
            int blockSize,
            int maxCached,
            long blockFilePosition,
            long blockFileSize,
            @Nonnull AsynchronousFileChannel channel,
            @Nonnull BlockBuffer buffer,
            int position,
            long sequence,
            long length,
            boolean last,
            @Nonnull Index index,
            @Nonnull Path slotDirPath,
            int maxPrefetched
    ) throws LedgerException, IOException {

        if (maxCached < 2) {
            throw new IllegalArgumentException();
        }

        if (blockFileSize < 2) {
            throw new IllegalArgumentException();
        }

        if (blockFilePosition < 0) {
            throw new IllegalArgumentException();
        }

        if (blockFilePosition >= blockFileSize) {
            throw new IllegalArgumentException();
        }

        this.index = index;
        slotHead = Slot.readSlots(this, maxPrefetched, blockSize, slotDirPath);

        messageWriter =
                new MessageWriter(blockSize, this, buffer.writeBuffer, position, MAX_CHUNK_DATA_SIZE, sequence, length,
                        last);

        this.blockBufferPool = blockBufferPool;

        this.blockSize = blockSize;
        this.maxCached = maxCached;

        queue = new QueueItem[maxCached];
        this.blockFilePosition = blockFilePosition;
        this.blockFileSize = blockFileSize;

        partialSize = position;
        final var item = new QueueItem(buffer);
        item.fileBlockPosition = blockFilePosition;
        item.pendingSize = partialSize;
        item.writingSize = partialSize;
        item.stableSize = partialSize;
        queue[0] = item;

        for (int i = 1; i < maxCached; i++) {
            queue[i] = new QueueItem(blockBufferPool.acquire());
        }

        this.channel = channel;

        readyMessageSequence = sequence;
        readyMessageOffset = length;
        readyMessageFinished = last;

        stableMessageSequence = sequence;
        stableMessageOffset = length;
        stableMessageFinished = last;

        currentPosition = 0;
        queueUsed = 1;

        final var max = Slot.max(slotHead);
        if (max != null && max.position.compareTo(sequence, length) > 0) {
            throw new LedgerException("Incorrect slot position");
        }

        adjustMinSlot();

        queueLock.set(false);// barrier
    }

    void adjustMinSlot() throws LedgerException {

        final var min = Slot.min(slotHead);
        slotMin = min;

        if (min == null) {
            firstNeededBlockFilePosition = -1;
        } else {
            if (min.position.equals(readyMessageSequence, readyMessageOffset)) {
                firstNeededBlockFilePosition = blockFilePosition;
            } else {
                firstNeededBlockFilePosition = index.findBlock(min.position);
                if (firstNeededBlockFilePosition == -1) {
                    throw new LedgerException("Incorrect slot position");
                }
            }
        }

        if (neededBlockWaiter != null) {
            LockSupport.unpark(neededBlockWaiter);
        }
    }

    public boolean lastPosition(@Nonnull Position position) {

        acquire(queueLock);
        try {
            position.set(readyMessageSequence, readyMessageOffset);
            return readyMessageFinished;
        } finally {
            release(queueLock);
        }
    }

    public boolean stablePosition(@Nonnull Position position) {

        acquire(queueLock);
        try {
            position.set(stableMessageSequence, stableMessageOffset);
            return stableMessageFinished;
        } finally {
            release(queueLock);
        }
    }

    void get(int skip, @Nonnull Slot slot) {

    }

    void get(long sequence, long offset, @Nonnull Slot slot) {

        if (slot.position.compareTo(sequence, offset) > 0) {
            throw new IllegalArgumentException();
        }

        final long blockNumber;
        final int nonresidentCount;

        acquire(queueLock);
        try {
            blockNumber = getFileBlockNumber(sequence, offset);
            if (blockNumber == -1) {
                throw new IllegalArgumentException();
            }

            final var delta = getDelta(blockNumber);
            final var count = (int) Math.min(delta, slot.maxPrefetched);
            nonresidentCount = (int) Math.min(delta - Math.min(delta, queueUsed), count);

            final var n = (int) ((currentPosition + 1 - delta + nonresidentCount) % maxCached);
            var j = n < 0 ? n + maxCached : n;

            for (var i = nonresidentCount; i < count; i++) {
                final var buffer = queue[j].buffer;
                buffer.refCount.incrementAndGet();
                slot.queue[i].buffer = buffer;
                j = (j + 1) % maxCached;
            }

            slot.currentPosition = count;
            slot.queueUsed = count;
            slot.blockFilePosition = (blockNumber + count - 1) % blockFileSize;
        } finally {
            release(queueLock);
        }

        var j = blockNumber;

        for (int i = 0; i < nonresidentCount; i++) {

            final var buffer = blockBufferPool.acquire();
            slot.queue[i].buffer = buffer;
            channel.read(buffer.writeBuffer, j * blockSize);// TODO:
            buffer.writeBuffer.clear();
            j = (j + 1) % blockFileSize;
        }

        slot.readPosition.set(sequence, offset);
    }

    private long getDelta(long blockNumber) {

        return blockFilePosition + 1 - (blockFilePosition < blockNumber ? blockNumber - blockFileSize : blockNumber);
    }

    private long getFileBlockNumber(long sequence, long offset) {

        if (sequence > readyMessageSequence) {
            return -1;
        } else if (sequence == readyMessageSequence) {
            if (offset > readyMessageOffset) {
                return -1;
            } else if (offset == readyMessageOffset) {
                return blockFilePosition;
            }
        }
        return index.findBlock(sequence, offset);
    }

    void releaseBuffer(@Nonnull BlockBuffer buffer) {

        if (buffer.refCount.decrementAndGet() == 0) {
            blockBufferPool.release(buffer);
        }
    }

    @Nonnull
    @Override
    public ByteBuffer fullBlock(
            long messageSequence,
            long accumulatedMessageLength,
            boolean messageFinished
    ) {
        final var item = queue[currentPosition];
        final var nextPosition = (currentPosition + 1) % maxCached;
        final var nextItem = queue[nextPosition];
        final var nextBlockFilePosition = (blockFilePosition + 1) % blockFileSize;

        acquire(queueLock);
        try {
            indexBlock(item);

            item.pendingSize = blockSize;
            item.pendingMessageSequence = messageSequence;
            item.pendingMessageOffset = accumulatedMessageLength;
            item.pendingMessageFinished = messageFinished;

            if (!item.writing && item.pendingSize > item.stableSize) {
                item.writing = true;
                tryWrite(item);
            }

            readyMessageSequence = messageSequence;
            readyMessageOffset = accumulatedMessageLength;
            readyMessageFinished = messageFinished;
            partialSize = blockSize;
            readerWaiters.wakeup();

            // wait until the block will be persisted to disk
            if (nextItem.fileBlockPosition != -1 && nextItem.stableSize != blockSize) {
                diskWaiter = Thread.currentThread();
                do {
                    release(queueLock);
                    try {
                        LockSupport.park();
                    } finally {
                        acquire(queueLock);
                    }
                } while (nextItem.stableSize != blockSize);
                diskWaiter = null;
            }

            if (firstNeededBlockFilePosition == nextBlockFilePosition) {
                try {
                    neededBlockWaiter = Thread.currentThread();
                    do {
                        release(queueLock);
                        try {
                            LockSupport.park();
                        } finally {
                            acquire(queueLock);
                        }
                    } while (firstNeededBlockFilePosition == nextBlockFilePosition);
                } finally {
                    neededBlockWaiter = null;
                }
            }

            var buffer = nextItem.buffer;

            while (true) {
                final var rc = buffer.refCount.get();
                // swap out subscribers
                if (rc > 1 && buffer.refCount.compareAndSet(rc, rc - 1)) {
                    buffer = blockBufferPool.acquire();
                    nextItem.buffer = buffer;
                    break;
                }
            }

            if (queueUsed < maxCached) {
                queueUsed++;
            }

            currentPosition = nextPosition;
            blockFilePosition = nextBlockFilePosition;
            partialSize = 0;
            nextItem.fileBlockPosition = nextBlockFilePosition;
            nextItem.writing = false;
            nextItem.pendingSize = 0;
            nextItem.writingSize = 0;
            nextItem.stableSize = 0;
            buffer.writeBuffer.clear();

            nextItem.prevWriting = tailWriting;
            if (tailWriting == null) {
                headWriting = nextItem;
                tailWriting = nextItem;
            } else {
                tailWriting.nextWriting = nextItem;
            }
            nextItem.nextWriting = null;

            index.remove(nextBlockFilePosition);

            return buffer.writeBuffer;
        } finally {
            release(queueLock);
        }
    }

    @Override
    public void partialBlock(
            long messageSequence,
            long accumulatedMessageLength,
            boolean messageFinished,
            int dataSize
    ) {
        acquire(queueLock);
        try {
            if (dataSize <= partialSize || dataSize >= blockSize) {
                throw new IllegalArgumentException();
            }

            final var item = queue[currentPosition];

            indexBlock(item);

            item.pendingSize = dataSize;
            item.pendingMessageSequence = messageSequence;
            item.pendingMessageOffset = accumulatedMessageLength;
            item.pendingMessageFinished = messageFinished;

            if (!item.writing && item.pendingSize > item.stableSize) {
                item.writing = true;
                tryWrite(item);
            }

            readyMessageSequence = messageSequence;
            readyMessageOffset = accumulatedMessageLength;
            readyMessageFinished = messageFinished;
            partialSize = blockSize;
            readerWaiters.wakeup();
        } finally {
            release(queueLock);
        }
    }

    private void indexBlock(@Nonnull QueueItem item) {

        if (partialSize == 0) {
            final var buffer = item.buffer.writeBuffer;
            index.put(item.fileBlockPosition, Math.abs(buffer.get(0)), buffer.get(8));
        }
    }

    private void tryWrite(@Nonnull QueueItem item) {

        item.writingSize = item.pendingSize;
        item.writingMessageSequence = item.pendingMessageSequence;
        item.writingMessageOffset = item.pendingMessageOffset;
        item.writingMessageFinished = item.pendingMessageFinished;
        final var buffer = item.buffer.diskBuffer;
        buffer.clear();
        channel.write(buffer, item.fileBlockPosition * blockSize, item, handler);
    }

    static void acquire(@Nonnull AtomicBoolean lock) {

        while (true) {
            if (lock.compareAndSet(false, true)) {
                break;
            }
        }
    }

    static void release(@Nonnull AtomicBoolean lock) {
        lock.set(false);
    }

    @Nonnull
    public MessageWriter getWriter() {
        return messageWriter;
    }
}
