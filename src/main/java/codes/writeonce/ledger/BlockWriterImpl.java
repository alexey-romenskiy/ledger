package codes.writeonce.ledger;

import codes.writeonce.concurrency.Bloom;
import codes.writeonce.concurrency.ParkingBee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static codes.writeonce.ledger.Slot.SLOT_SUFFIX;
import static codes.writeonce.ledger.ThrowableUtils.str;

public class BlockWriterImpl implements BlockWriter {

    private static final AtomicLong INDEXER_THREAD_SEQUENCE = new AtomicLong();

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Nonnull
    private final TreeMap<Long, BlockBuffer> cachedBlockMap = new TreeMap<>();

    final long topicId;

    final Bloom queuePositionBloom = new Bloom();

    final Bloom indexBloom = new Bloom();

    private final int blockSize;

    private final int queueSize;

    @Nonnull
    private final byte[] zeroes;

    @Nullable
    private final ExecutorService executor;

    @Nullable
    Slot slotHead;

    private final int maxPrefetched;

    private final Path slotDirPath;

    private int queueUsed;

    @Nonnull
    private final BlockBuffer[] queue;

    @Nonnull
    private final Pool<BlockBuffer> blockBufferPool;

    @Nonnull
    private final AtomicReference<Thread> writerThread = new AtomicReference<>();

    boolean fileSpaceBlocked;

    @Nonnull
    private final AsynchronousFileChannel channel;

    private long blockFilePosition;

    private long lastIndexedFileBlock;

    private long lastIndexedSequence;

    private long lastIndexedOffset;

    private int queuePosition;

    private int unstablePosition;

    private long firstNeededBlockFilePosition = -1;

    final long blockFileSize;

    @Nonnull
    private final CompletionHandler<Integer, BlockBuffer> handler = new CompletionHandlerImpl();

    volatile Throwable writeFailed;

    private final Bloom writeFailedBloom = new Bloom();

    public volatile long stableSequence;

    public volatile long stableOffset;

    public final Bloom stableBloom = new Bloom();

    public volatile long readySequence;

    public volatile long readyOffset;

    private long lastBlockSequence;

    private long lastBlockOffset;

    public final Bloom dataBloom = new Bloom();

    private final AtomicBoolean queueLock = new AtomicBoolean();

    private final AtomicInteger pendingIndexCount = new AtomicInteger(0);

    private int lastIndexedDistance;

    private final AtomicBoolean indexerQueueLock = new AtomicBoolean();

    private final AtomicBoolean slotsLock = new AtomicBoolean();

    @Nonnull
    private final Index index;

    @Nonnull
    private final long[] indexerQueue;

    @Nonnull
    private final Thread indexerThread;

    private int indexerHead;

    private volatile Thread indexQueueWaiterThread;

    final AtomicBoolean shutdown = new AtomicBoolean();

    private final CompletableFuture<Boolean> shutdownFuture = new CompletableFuture<>();

    private volatile boolean indexCompleted;

    private final AtomicInteger pendingWritesCount = new AtomicInteger();

    @Nonnull
    private final LedgerMetrics ledgerMetrics;

    public BlockWriterImpl(
            long topicId,
            @Nonnull final Pool<BlockBuffer> blockBufferPool,
            final int blockSize,
            final int maxCached,
            final long blockFilePosition,
            final long ledgerBlockCount,
            @Nonnull final BlockBuffer buffer,
            final int position,
            @Nonnull final AsynchronousFileChannel channel,
            @Nonnull final Index index,
            final long lastIndexedFileBlock,
            final int lastIndexedDistance,
            final int maxPrefetched,
            @Nonnull final Path slotDirPath,
            final long readyMessageSequence,
            final long readyMessageOffset,
            final long lastBlockSequence,
            final long lastBlockOffset,
            @Nonnull byte[] zeroes,
            @Nullable ExecutorService executor,
            @Nonnull LedgerMetrics ledgerMetrics
    ) throws LedgerException, IOException {

        if (maxCached < 2) {
            throw new IllegalArgumentException();
        }

        if (ledgerBlockCount < 2) {
            throw new IllegalArgumentException();
        }

        if (blockFilePosition < 0) {
            throw new IllegalArgumentException();
        }

        if (blockFilePosition >= ledgerBlockCount) {
            throw new IllegalArgumentException();
        }

        this.topicId = topicId;
        this.zeroes = zeroes;
        this.executor = executor;
        this.ledgerMetrics = ledgerMetrics;
        this.readySequence = readyMessageSequence;
        this.readyOffset = readyMessageOffset;
        this.blockFilePosition = blockFilePosition;
        this.index = index;
        this.lastBlockSequence = lastBlockSequence;
        this.lastBlockOffset = lastBlockOffset;
        this.lastIndexedSequence = readyMessageSequence;
        this.lastIndexedOffset = readyMessageOffset;

        slotHead = Slot.readSlots(this, maxPrefetched, blockSize, slotDirPath);

        final var min = Slot.min(slotHead);
        if (min != null) {
            if (index.getFilledBlockCount() > 0) {
                final var firstPosition = new Position(1, 0);
                index.get(index.getFirstFilledBlock(), firstPosition);
                if (min.position.compareTo(firstPosition) < 0) {
                    throw new LedgerException("Incorrect slot position");
                }
            } else if (min.position.compareTo(readyMessageSequence, readyMessageOffset) < 0) {
                throw new LedgerException("Incorrect slot position");
            }
        }

        final var max = Slot.max(slotHead);
        if (max != null && max.position.compareTo(readyMessageSequence, readyMessageOffset) > 0) {
            throw new LedgerException("Incorrect slot position");
        }

        logger.info("LEDGER#{}: Initial firstNeededBlockFilePosition={}", topicId,
                updateFirstNeededBlockFilePosition());

        this.stableSequence = readyMessageSequence;
        this.stableOffset = readyMessageOffset;

        this.channel = channel;
        this.blockBufferPool = blockBufferPool;
        this.blockSize = blockSize;
        this.queueSize = maxCached;
        this.queueUsed = 1;
        this.lastIndexedFileBlock = lastIndexedFileBlock;
        this.lastIndexedDistance = lastIndexedDistance;
        this.blockFileSize = ledgerBlockCount;

        queue = new BlockBuffer[maxCached];

        buffer.index = 0;
        buffer.blockFilePosition = blockFilePosition;
        buffer.availableDataLength = position;
        buffer.readySequence = readyMessageSequence;
        buffer.readyOffset = readyMessageOffset;
        buffer.writingSequence = readyMessageSequence;
        buffer.writingOffset = readyMessageOffset;
        buffer.stableSequence = readyMessageSequence;
        buffer.stableOffset = readyMessageOffset;
        buffer.writingSize = position;
        buffer.stableSize = position;
        queue[0] = buffer;

        for (int i = 1; i < maxCached; i++) {
            final var item = blockBufferPool.acquire();
            item.index = i;
            item.stableSize = blockSize;
            queue[i] = item;
        }

        this.queuePosition = 0;
        this.unstablePosition = 0;

        indexerQueue = new long[maxCached * 5];
        indexerThread =
                new Thread(this::indexerLoop, "indexer#" + topicId + "-" + INDEXER_THREAD_SEQUENCE.incrementAndGet());

        this.maxPrefetched = maxPrefetched;
        this.slotDirPath = slotDirPath;

        queueLock.set(false);// barrier
        indexerQueueLock.set(false);// barrier
        slotsLock.set(false);// barrier
        indexerThread.start();
    }

    long updateFirstNeededBlockFilePosition() {

        final var min = Slot.min(slotHead);
        final var r = min == null ? -1 : min.firstNeededBlockNumber;

        if (firstNeededBlockFilePosition != r) {

            if (firstNeededBlockFilePosition != -1) {
                if (r == -1) {
                    invalidateCachedBlocks(0, blockFileSize);
                } else if (firstNeededBlockFilePosition < r) {
                    invalidateCachedBlocks(firstNeededBlockFilePosition, r);
                } else {
                    invalidateCachedBlocks(firstNeededBlockFilePosition, blockFileSize);
                    if (r != 0) {
                        invalidateCachedBlocks(0, r);
                    }
                }
            }

            firstNeededBlockFilePosition = r;

            if (fileSpaceBlocked) {
                final var thread = writerThread.get();
                if (thread != null) {
                    LockSupport.unpark(thread);
                }
            }
        }

        return r;
    }

    private void invalidateCachedBlocks(long fromIndex, long toIndex) {

        final var iterator = cachedBlockMap.subMap(fromIndex, toIndex).values().iterator();

        while (iterator.hasNext()) {
            final var item = iterator.next();
            item.cached = false;
            iterator.remove();
        }
    }

    @Nonnull
    @Override
    public ByteBuffer fullBlock(long messageSequence, long accumulatedMessageLength) {

        if (!writerThread.compareAndSet(null, Thread.currentThread())) {
            throw new IllegalStateException();
        }

        try {
            final var position = queuePosition;

            final var item = queue[position];

            logger.info("LEDGER#{}: fullBlock index={} blockNumber={} availableDataLength={} sequence={} offset={}",
                    topicId, item.index, item.blockFilePosition, item.availableDataLength, messageSequence,
                    accumulatedMessageLength);
            indexBlock(item, item.availableDataLength, messageSequence, accumulatedMessageLength);

            final var nextBlockFilePosition = (blockFilePosition + 1) % blockFileSize;

            final var nextQueuePosition = (position + 1) % queueSize;
            var nextItem = queue[nextQueuePosition];

            acquire();
            try {
                if (shutdown.get()) {
                    throw new IllegalStateException();
                }

                item.readySequence = messageSequence;
                item.readyOffset = accumulatedMessageLength;
                updateReady(messageSequence, accumulatedMessageLength);
                item.availableDataLength = blockSize;
                dataBloom.ready();
                item.dataBloom.ready();

                blockWaitStable(nextItem);
                blockWaitFileSpace(nextBlockFilePosition);

                if (nextItem.refCount == 1) {
                    nextItem.writeBuffer.clear();
                    nextItem.writeBuffer.put(0, zeroes);
                } else {
                    // swap out for subscribers
                    nextItem = blockBufferPool.acquire();
                    nextItem.index = nextQueuePosition;
                    queue[nextQueuePosition] = nextItem;
                }

                nextItem.availableDataLength = 0;
                nextItem.blockFilePosition = nextBlockFilePosition;
                nextItem.writingSize = 0;
                nextItem.stableSize = 0;

                blockFilePosition = nextBlockFilePosition;
                queuePosition = nextQueuePosition;

                if (queueUsed < queueSize) {
                    queueUsed++;
                }

                lastIndexedDistance++;

                lastBlockSequence = messageSequence;
                lastBlockOffset = accumulatedMessageLength;
                tryWrite(item);
            } finally {
                release();
            }

            queuePositionBloom.ready();

            return nextItem.writeBuffer;
        } finally {
            writerThread.set(null);
        }
    }

    @Override
    public void partialBlock(long messageSequence, long accumulatedMessageLength, int dataSize) {

        if (!writerThread.compareAndSet(null, Thread.currentThread())) {
            throw new IllegalStateException();
        }

        try {
            final var position = queuePosition;
            final var item = queue[position];

            final var availableDataLength = item.availableDataLength;

            if (dataSize <= availableDataLength || dataSize >= blockSize) {
                throw new IllegalArgumentException();
            }

            logger.info("LEDGER#{}: partialBlock index={} blockNumber={} availableDataLength={} sequence={} offset={}",
                    topicId, item.index, item.blockFilePosition, availableDataLength, messageSequence,
                    accumulatedMessageLength);
            indexBlock(item, availableDataLength, messageSequence, accumulatedMessageLength);

            acquire();
            try {
                if (shutdown.get()) {
                    throw new IllegalStateException();
                }

                item.readySequence = messageSequence;
                item.readyOffset = accumulatedMessageLength;
                updateReady(messageSequence, accumulatedMessageLength);
                item.availableDataLength = dataSize;
                dataBloom.ready();
                item.dataBloom.ready();

                tryWrite(item);
            } finally {
                release();
            }
        } finally {
            writerThread.set(null);
        }
    }

    private void indexBlock(
            @Nonnull BlockBuffer item,
            int availableDataLength,
            long messageSequence,
            long accumulatedMessageLength
    ) {
        if (availableDataLength == 0) {
            final var writeBuffer = item.writeBuffer;
            final var blockNumber = item.blockFilePosition;
            final var sequence = Math.abs(writeBuffer.getLong(0));
            final var offset = writeBuffer.getLong(8);
            logger.info("LEDGER#{}: advanceIndex index={} blockNumber={} sequence={} offset={}", topicId, item.index,
                    blockNumber, sequence, offset);
            advanceIndex(
                    blockNumber,
                    sequence,
                    offset,
                    messageSequence,
                    accumulatedMessageLength
            );
        }
    }

    private void updateReady(long messageSequence, long accumulatedMessageLength) {

        readyOffset = 0;
        readySequence = messageSequence;
        readyOffset = accumulatedMessageLength;
    }

    public void readReadyPosition(@Nonnull Position position) {

        if (position.sequence == readySequence) {
            position.offset = Math.max(readyOffset, position.offset);
        } else {
            position.sequence = readySequence;
            position.offset = readyOffset;
        }
    }

    public boolean awaitStablePosition(long sequence, long offset) {

        if (checkStableSequence(sequence, offset)) {
            return true;
        }

        acquire();
        try {
            if (checkStableSequence(sequence, offset)) {
                return true;
            }
            if (shutdown.get()) {
                return false;
            }
            final var bee = new ParkingBee(stableBloom, Thread.currentThread());
            do {
                bee.ready();
                release();
                try {
                    LockSupport.park();
                } finally {
                    acquire();
                }
                if (checkStableSequence(sequence, offset)) {
                    return true;
                }
                if (shutdown.get()) {
                    return false;
                }
            } while (true);
        } finally {
            release();
        }
    }

    private boolean checkStableSequence(long sequence, long offset) {

        final var rs = stableSequence;
        return rs > sequence || rs == sequence && stableOffset >= offset;
    }

    public boolean awaitReadyPosition(long sequence, long offset) {

        if (checkReadySequence(sequence, offset)) {
            return true;
        }

        acquire();
        try {
            if (checkReadySequence(sequence, offset)) {
                return true;
            }
            if (shutdown.get()) {
                return false;
            }
            final var bee = new ParkingBee(dataBloom, Thread.currentThread());
            do {
                bee.ready();
                release();
                try {
                    LockSupport.park();
                } finally {
                    acquire();
                }
                if (checkReadySequence(sequence, offset)) {
                    return true;
                }
                if (shutdown.get()) {
                    return false;
                }
            } while (true);
        } finally {
            release();
        }
    }

    private boolean checkReadySequence(long sequence, long offset) {

        final var rs = readySequence;
        return rs > sequence || rs == sequence && readyOffset >= offset;
    }

    private void notifyStable(@Nonnull BlockBuffer item) {

        stableOffset = 0;
        stableSequence = item.stableSequence;
        stableOffset = item.stableOffset;
        stableBloom.ready();
    }

    public void readStablePosition(@Nonnull Position position) {

        acquire();
        try {
            position.sequence = stableSequence;
            position.offset = stableOffset;
        } finally {
            release();
        }
    }

    private void tryWrite(@Nonnull BlockBuffer item) {

        if (!item.writing) {
            item.writing = true;
            tryWrite2(item);
        }
    }

    private void tryWrite2(@Nonnull BlockBuffer item) {

        item.writingSize = item.availableDataLength;
        item.writingSequence = item.readySequence;
        item.writingOffset = item.readyOffset;
        final var diskBuffer = item.diskBuffer;
        diskBuffer.clear();
        final var position = item.blockFilePosition * blockSize;
        release();
        try {
            pendingWritesCount.incrementAndGet();
            item.writingStartNanos = System.nanoTime();
            channel.write(diskBuffer, position, item, handler);
        } finally {
            acquire();
        }
    }

    private void blockWaitFileSpace(long nextBlockFilePosition) {

        while (true) {
            if (firstNeededBlockFilePosition != nextBlockFilePosition) {
                fileSpaceBlocked = false;
                break;
            }
            if (shutdown.get()) {
                throw new IllegalStateException();
            }
            fileSpaceBlocked = true;
            release();
            try {
                LockSupport.park();
            } finally {
                acquire();
            }
        }
    }

    private void blockWaitStable(@Nonnull BlockBuffer item) {

        if (item.stableSize != blockSize) {
            final var writerBee = new ParkingBee(item.stabBloom, Thread.currentThread());
            do {
                var cause = item.writeFailed;
                if (cause != null) {
                    throw new RuntimeException(cause);
                }
                writerBee.ready();
                if (item.stableSize == blockSize) {
                    break;
                }
                cause = item.writeFailed;
                if (cause != null) {
                    throw new RuntimeException(cause);
                }
                release();
                try {
                    LockSupport.park();
                } finally {
                    acquire();
                }
            } while (item.stableSize != blockSize);
        }
    }

    void acquire() {

        while (true) {
            if (queueLock.compareAndSet(false, true)) {
                break;
            }
        }
    }

    void release() {
        queueLock.set(false);
    }

    void clean(@Nonnull BlockBuffer[] queue, int start, int count) {

        acquire();
        try {
            for (int i = 0; i < count; i++) {
                final var n = (start + i) % queue.length;
                final var item = queue[n];
                queue[n] = null;
                tryRelease(item);
            }
        } finally {
            release();
        }
    }

    int get(long blockNumber, @Nonnull BlockBuffer[] queue, int start, int maxPrefetched, long delta) {

        final int nonresidentCount;
        final int countPrefetched;

        final var deltaPlus = delta + 1;
        countPrefetched = (int) Math.min(deltaPlus, maxPrefetched);
        nonresidentCount = (int) Math.min(deltaPlus - Math.min(deltaPlus, queueUsed), countPrefetched);

        final var n = (int) ((queuePosition - delta + nonresidentCount) % queueSize);
        var j = n < 0 ? n + queueSize : n;

        for (var i = nonresidentCount; i < countPrefetched; i++) {
            final var item = this.queue[j];
            item.refCount++;
            queue[(start + i) % queue.length] = item;
            j = (j + 1) % queueSize;
        }

        var k = blockNumber;

        for (int i = 0; i < nonresidentCount; i++) {
            read(queue, (start + i) % queue.length, k);
            k = (k + 1) % blockFileSize;
        }

        return countPrefetched;
    }

    private void read(@Nonnull BlockBuffer[] queue, int queueIndex, long blockFilePosition) {

        BlockBuffer item = cachedBlockMap.get(blockFilePosition);

        if (item != null) {
            queue[queueIndex] = item;
            item.refCount++;
            return;
        }

        item = blockBufferPool.acquire();
        item.blockFilePosition = blockFilePosition;
        item.stableSize = blockSize;
        queue[queueIndex] = item;
        item.refCount++; // 1 -> 2
        item.cached = true;
        cachedBlockMap.put(blockFilePosition, item);

        release();
        try {
            channel.read(item.writeBuffer, blockFilePosition * blockSize, item, new CompletionHandler<>() {
                @Override
                public void completed(Integer result, BlockBuffer item) {

                    item.availableDataLength = blockSize;
                    item.dataBloom.ready();
                    tryReleaseGuarded(item);
                }

                @Override
                public void failed(Throwable throwable, BlockBuffer item) {

                    item.readFailed = true;
                    item.dataBloom.ready();
                    tryReleaseGuarded(item);
                }
            });
        } finally {
            acquire();
        }
    }

    private void tryReleaseGuarded(@Nonnull BlockBuffer item) {

        acquire();
        try {
            tryRelease(item);
        } finally {
            release();
        }
    }

    private void tryRelease(@Nonnull BlockBuffer item) {
        if (--item.refCount == 0) {
            if (item.cached) {
                cachedBlockMap.remove(item.blockFilePosition);
            }
            blockBufferPool.release(item);
        }
    }

    /**
     * @param blockNumber file block number
     * @return <code>0</code> if this <code>blockNumber</code> is current unfinished, positive otherwise
     */
    long getDelta(long blockNumber) {

        return blockFilePosition < blockNumber
                ? blockFilePosition + blockFileSize - blockNumber
                : blockFilePosition - blockNumber;
    }

    int getStableSize(long blockNumber) {

        return blockFilePosition == blockNumber ? queue[queuePosition].stableSize : blockSize;
    }

    long getFileBlockNumber(long sequence, long offset, @Nonnull ParkingBee indexBee) {

        final var rs = readySequence;
        if (sequence > rs) {
            return -1;
        } else if (sequence == rs) {
            acquire();
            try {
                if (sequence == readySequence) {
                    final var ro = readyOffset;
                    if (offset > ro) {
                        return -1;
                    } else if (offset == ro) {
                        return blockFilePosition;
                    }
                }
                if (isLastBlock(sequence, offset)) {
                    return blockFilePosition;
                }
                if (waitIndexed(sequence, offset, indexBee)) {
                    return -1;
                }
            } finally {
                release();
            }
        } else {
            acquire();
            try {
                if (isLastBlock(sequence, offset)) {
                    return blockFilePosition;
                }
                if (waitIndexed(sequence, offset, indexBee)) {
                    return -1;
                }
            } finally {
                release();
            }
        }
        return findBlock(sequence, offset);
    }

    private boolean waitIndexed(long sequence, long offset, @Nonnull ParkingBee indexBee) {

        while (true) {
            if (isIndexed(sequence, offset)) {
                break;
            }
            indexBee.ready();
            if (isIndexed(sequence, offset)) {
                break;
            }
            if (indexCompleted) {
                return true;
            }
            release();
            try {
                LockSupport.park();
            } finally {
                acquire();
            }
        }

        return false;
    }

    private boolean isLastBlock(long sequence, long offset) {
        return sequence > lastBlockSequence || sequence == lastBlockSequence && offset >= lastBlockOffset;
    }

    private boolean isIndexed(long sequence, long offset) {
        return sequence < lastIndexedSequence || sequence == lastIndexedSequence && offset <= lastIndexedOffset;
    }

    private long findBlock(long sequence, long offset) {

        acquireIndex();
        try {
            return index.findBlock(sequence, offset);
        } finally {
            releaseIndex();
        }
    }

    @Nullable
    Slot slot(long id) {

        acquireSlots();
        try {
            return findSlot(id);
        } finally {
            releaseSlots();
        }
    }

    @Nullable
    private Slot findSlot(long id) {

        final var head = slotHead;
        if (head == null) {
            return null;
        }

        var slot = head;
        do {
            if (slot.id == id) {
                return slot;
            }
            slot = slot.next;
        } while (slot != head);

        return null;
    }

    boolean deleteSlot(long id) {

        acquireSlots();
        try {
            final var slot = findSlot(id);
            if (slot == null) {
                return false;
            }
            slot.deleteSlot();
            if (slot.next == slot) {
                slotHead = null;
            } else {
                final var prev = slot.prev;
                final var next = slot.next;
                prev.next = next;
                next.prev = prev;
                if (slotHead == slot) {
                    slotHead = next;
                }
            }
            acquire();
            try {
                logger.info(
                        "LEDGER#{}: Deleted slot id={} sequence={} offset={} firstNeededBlockNumber={} firstNeededBlockFilePosition={}",
                        topicId, slot.id, slot.position.sequence, slot.position.offset, slot.firstNeededBlockNumber,
                        updateFirstNeededBlockFilePosition());
            } finally {
                release();
            }

            return true;
        } finally {
            releaseSlots();
        }
    }

    @Nonnull
    Slot createSlot() {

        acquireSlots();
        try {
            final Slot slot;

            acquire();
            try {
                slot = createSlot(readySequence, readyOffset, blockFilePosition);
            } finally {
                release();
            }

            slot.writeSlot();
            return slot;
        } finally {
            releaseSlots();
        }
    }

    @Nullable
    Slot createSlot(long sequence, long offset) {

        final var rs = readySequence;
        if (sequence > rs) {
            return null;
        }

        final var indexBee = new ParkingBee(indexBloom, Thread.currentThread());
        acquireSlots();
        try {
            Slot slot = null;

            acquire();
            try {
                if (sequence == rs) {
                    if (sequence == readySequence) {
                        final var ro = readyOffset;
                        if (offset > ro) {
                            return null;
                        } else if (offset == ro) {
                            slot = createSlot(sequence, offset, blockFilePosition);
                        }
                    } else if (isLastBlock(sequence, offset)) {
                        slot = createSlot(sequence, offset, blockFilePosition);
                    } else if (waitIndexed(sequence, offset, indexBee)) {
                        return null;
                    }
                } else if (isLastBlock(sequence, offset)) {
                    slot = createSlot(sequence, offset, blockFilePosition);
                } else if (waitIndexed(sequence, offset, indexBee)) {
                    return null;
                }
            } finally {
                release();
            }

            if (slot == null) {
                acquireIndex();
                try {
                    final var blockNumber = index.findBlock(sequence, offset);
                    if (blockNumber == -1) {
                        return null;
                    }

                    acquire();
                    try {
                        final var foundOffset = blockNumber - index.getFirstFilledBlock();
                        final var foundDistance = index.getFilledBlockCount() + lastIndexedDistance - foundOffset;
                        if (foundDistance > blockFileSize) {
                            return null;
                        }

                        slot = createSlot(sequence, offset, blockNumber);
                    } finally {
                        release();
                    }
                } finally {
                    releaseIndex();
                }
            }

            slot.writeSlot();
            return slot;
        } finally {
            releaseSlots();
        }
    }

    @Nonnull
    private Slot createSlot(long sequence, long offset, long blockNumber) {

        final Slot slot;

        if (slotHead == null) {
            slot = createSlot(sequence, offset, 1, blockNumber);
            slotHead = slot;
            slot.prev = slot;
            slot.next = slot;
        } else {
            long maxId = slotHead.id;
            var item = slotHead;
            while (true) {
                item = item.next;
                if (item == slotHead) {
                    break;
                }
                if (item.id > maxId) {
                    maxId = item.id;
                }
            }
            slot = createSlot(sequence, offset, maxId + 1, blockNumber);
            final var tail = slotHead.prev;
            slot.prev = tail;
            slot.next = slotHead;
            slotHead.prev = slot;
            tail.next = slot;
        }

        logger.info(
                "LEDGER#{}: New slot id={} sequence={} offset={} firstNeededBlockNumber={} firstNeededBlockFilePosition={}",
                topicId, slot.id, sequence, offset, slot.firstNeededBlockNumber, updateFirstNeededBlockFilePosition());
        return slot;
    }

    @Nonnull
    private Slot createSlot(long sequence, long offset, long id, long blockNumber) {

        return new Slot(this, maxPrefetched, blockSize, id, slotDirPath.resolve(id + SLOT_SUFFIX),
                new Position(sequence, offset), blockNumber);
    }

    private void advanceIndex(
            long blockNumber,
            long sequence,
            long offset,
            long messageSequence,
            long accumulatedMessageLength
    ) {
        blockFreeIndex();

        final var p = indexerHead;
        final var n = p * 5;
        indexerQueue[n] = blockNumber;
        indexerQueue[n + 1] = sequence;
        indexerQueue[n + 2] = offset;
        indexerQueue[n + 3] = messageSequence;
        indexerQueue[n + 4] = accumulatedMessageLength;
        indexerHead = (p + 1) % queueSize;

        if (pendingIndexCount.getAndIncrement() == 0) {
            LockSupport.unpark(indexerThread);
        }
    }

    private void blockFreeIndex() {

        if (pendingIndexCount.get() == queueSize) {
            indexQueueWaiterThread = Thread.currentThread();
            while (pendingIndexCount.get() == queueSize) {
                if (shutdown.get()) {
                    throw new IllegalStateException();
                }
                LockSupport.park();
            }
            indexQueueWaiterThread = null;
        }
    }

    private void indexerLoop() {

        try {
            var indexerTail = 0;
            var c = pendingIndexCount.get();

            while (true) {
                if (c == 0) {
                    if (shutdown.get()) {
                        c = pendingIndexCount.get();
                        if (c == 0) {
                            break;
                        }
                    } else {
                        LockSupport.park();
                        c = pendingIndexCount.get();
                    }
                } else {
                    long blockNumber;
                    int n;

                    acquireIndex();
                    try {
                        var c3 = c;
                        do {
                            n = indexerTail * 5;
                            blockNumber = indexerQueue[n];
                            final var sequence = indexerQueue[n + 1];
                            final var offset = indexerQueue[n + 2];
                            logger.info("LEDGER#{}: index.put index={} blockNumber={} sequence={} offset={}", topicId,
                                    indexerTail, blockNumber, sequence, offset);
                            index.put(blockNumber, sequence, offset);
                            indexerTail = (indexerTail + 1) % queueSize;
                        } while (--c3 != 0);

                        acquire();
                        try {
                            lastIndexedDistance -= c;
                            lastIndexedFileBlock = blockNumber;
                            lastIndexedSequence = indexerQueue[n + 3];
                            lastIndexedOffset = indexerQueue[n + 4];
                        } finally {
                            release();
                        }
                    } finally {
                        releaseIndex();
                    }

                    indexBloom.ready();

                    final var c2 = pendingIndexCount.getAndAdd(-c);
                    if (c2 == queueSize) {
                        final var thread = indexQueueWaiterThread;
                        if (thread != null) {
                            LockSupport.unpark(thread);
                        }
                    }

                    c = c2 - c;
                }
            }

            index.close();
        } finally {
            indexCompleted = true;
            indexBloom.ready();
        }
    }

    boolean close() {

        if (shutdown.getAndSet(true)) {
            try {
                return shutdownFuture.get();
            } catch (InterruptedException e) {
                logger.error("LEDGER#{}: Failed to wait for shutdown", topicId, e);
                Thread.currentThread().interrupt();
                return false;
            } catch (ExecutionException e) {
                logger.error("LEDGER#{}: Failed to wait for shutdown", topicId, e);
                return false;
            }
        }

        try {
            logger.info("LEDGER#{}: Initializing shutdown", topicId);

            LockSupport.unpark(indexerThread);

            final var thread = writerThread.get();
            if (thread != null) {
                LockSupport.unpark(thread);
            }

            acquire();
            try {
                stableBloom.ready();
                dataBloom.ready();
                queue[queuePosition].dataBloom.ready();
                queuePositionBloom.ready();
                indexBloom.ready();
            } finally {
                release();
            }

            if (!indexCompleted) {
                final var bee = new ParkingBee(indexBloom, Thread.currentThread());
                while (true) {
                    bee.ready();
                    if (indexCompleted) {
                        break;
                    }
                    LockSupport.park();
                }
            }

            final var clean = isClean();

            try {
                channel.close();
            } catch (IOException e) {
                logger.error("LEDGER#{}: Failed to close the ledger data channel: {}", topicId, str(e), e);
            }

            if (executor != null) {
                executor.shutdown();
            }

            shutdownFuture.complete(clean);
            return clean;
        } catch (Throwable e) {
            shutdownFuture.completeExceptionally(e);
            throw e;
        }
    }

    private boolean isClean() {

        acquire();
        try {
            return isIndexClean() && isWriteClean();
        } finally {
            release();
        }
    }

    private boolean isWriteClean() {

        for (int i = 0; i < queueSize; i++) {
            final var item = queue[i];
            if (item.writing) {
                final var bee = new ParkingBee(item.stabBloom, Thread.currentThread());
                while (true) {
                    bee.ready();
                    if (writeFailed != null || !item.writing) {
                        break;
                    }
                    release();
                    try {
                        LockSupport.park();
                    } finally {
                        acquire();
                    }
                    if (writeFailed != null || !item.writing) {
                        break;
                    }
                }
                if (writeFailed != null) {
                    break;
                }
            }
        }

        final var clean = writeFailed == null;

        if (clean) {
            logger.info("LEDGER#{}: All blocks written", topicId);
        } else {
            logger.error("LEDGER#{}: Not all blocks written", topicId);
        }

        return clean;
    }

    private boolean isIndexClean() {

        final var item = queue[queuePosition];
        final long lastBlockNumber;

        if (item.availableDataLength == 0) {
            if (queueUsed > 1) {
                lastBlockNumber = (blockFilePosition + blockFileSize - 1) % blockFileSize;
            } else {
                lastBlockNumber = -1;
            }
        } else {
            lastBlockNumber = blockFilePosition;
        }

        final var clean = lastIndexedFileBlock == lastBlockNumber;

        if (clean) {
            logger.info("LEDGER#{}: Index flushed: lastBlockNumber={}", topicId, lastBlockNumber);
        } else {
            logger.error("LEDGER#{}: Index inconsistent: lastBlockNumber={} lastIndexedFileBlock={}", topicId,
                    lastBlockNumber, lastIndexedFileBlock);
        }

        return clean;
    }

    private void acquireIndex() {

        while (true) {
            if (indexerQueueLock.compareAndSet(false, true)) {
                break;
            }
        }
    }

    private void releaseIndex() {
        indexerQueueLock.set(false);
    }

    void acquireSlots() {

        while (true) {
            if (slotsLock.compareAndSet(false, true)) {
                break;
            }
        }
    }

    void releaseSlots() {
        slotsLock.set(false);
    }

    long getBlockFilePosition() {

        acquire();
        try {
            return blockFilePosition;
        } finally {
            release();
        }
    }

    long getBlockFileRemained() {

        acquire();
        try {
            return (firstNeededBlockFilePosition == -1
                    ? blockFileSize
                    : firstNeededBlockFilePosition > blockFilePosition
                            ? firstNeededBlockFilePosition - blockFilePosition
                            : blockFileSize - blockFilePosition + firstNeededBlockFilePosition) - 1;
        } finally {
            release();
        }
    }

    public int getUnstablePosition() {

        acquire();
        try {
            return unstablePosition;
        } finally {
            release();
        }
    }

    public int getUnstableRemaining() {

        acquire();
        try {
            return (queuePosition < unstablePosition
                    ? queuePosition - unstablePosition + queueSize
                    : queuePosition - unstablePosition) + 1;
        } finally {
            release();
        }
    }

    public int getPendingWritesCount() {
        return pendingWritesCount.get();
    }

    int getQueuePosition() {

        acquire();
        try {
            return queuePosition;
        } finally {
            release();
        }
    }

    @Nonnull
    Map<Long, Map<String, Object>> getSlotInfos() {

        final var slotIInfos = new HashMap<Long, Map<String, Object>>();

        acquireSlots();
        try {
            if (slotHead == null) {
                return slotIInfos;
            }

            var item = slotHead;

            do {
                slotIInfos.put(item.id, item.getInfo());
                item = item.next;
            } while (item != slotHead);
        } finally {
            releaseSlots();
        }

        return slotIInfos;
    }

    int getBlockSize() {
        return blockSize;
    }

    int getQueueSize() {
        return queueSize;
    }

    int getQueueRemained() {

        acquire();
        try {
            return queueSize - queueUsed;
        } finally {
            release();
        }
    }

    long getFirstNeededBlockFilePosition() {

        acquire();
        try {
            return firstNeededBlockFilePosition;
        } finally {
            release();
        }
    }

    long getBlockFileSize() {
        return blockFileSize;
    }

    private class CompletionHandlerImpl implements CompletionHandler<Integer, BlockBuffer> {

        @Override
        public void completed(Integer result, BlockBuffer item) {

            final var elapsed = System.nanoTime() - item.writingStartNanos;
            ledgerMetrics.writeDelay(elapsed);
            logger.info("LEDGER#{}: stable sequence={} offset={} blockPosition={} blockSize={} delay={}ns", topicId,
                    item.writingSequence, item.writingOffset, item.blockFilePosition, item.writingSize, elapsed);

            pendingWritesCount.decrementAndGet();

            acquire();
            try {
                final var size = item.writingSize;

                item.stableSize = size;
                item.stableSequence = item.writingSequence;
                item.stableOffset = item.writingOffset;

                var index = item.index;
                if (index == unstablePosition) {
                    if (size == blockSize) {
                        var i = item;
                        while (true) {
                            final var tail = index == queuePosition;
                            index = (index + 1) % queueSize;
                            if (tail) {
                                break;
                            }
                            final var nextItem = queue[index];
                            final var nextSize = nextItem.stableSize;
                            if (nextSize == 0) {
                                break;
                            }
                            i = nextItem;
                            if (nextSize != blockSize) {
                                break;
                            }
                        }
                        unstablePosition = index;
                        notifyStable(i);
                    } else {
                        notifyStable(item);
                    }
                }

                if (item.availableDataLength > size) {
                    tryWrite2(item);
                } else {
                    item.writing = false;
                }
            } finally {
                release();
            }

            item.stabBloom.ready();
        }

        @Override
        public void failed(Throwable throwable, BlockBuffer item) {

            pendingWritesCount.decrementAndGet();

            writeFailed = throwable;
            item.writeFailed = throwable;
            writeFailedBloom.ready();
            item.stabBloom.ready();
        }
    }
}
