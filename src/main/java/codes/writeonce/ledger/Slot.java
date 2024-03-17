package codes.writeonce.ledger;

import codes.writeonce.concurrency.Hive;
import codes.writeonce.concurrency.HiveBee;
import codes.writeonce.concurrency.ParkingBee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static codes.writeonce.ledger.MessageWriter.HEADER_SIZE;
import static codes.writeonce.ledger.MessageWriter.TRAILER_SIZE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class Slot {

    private static final int PREFETCH_FACTOR = 4;

    static final String SLOT_SUFFIX = ".slot";

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final AtomicBoolean positionLock = new AtomicBoolean();

    @Nonnull
    private final BlockWriterImpl blockWriter;

    private final int maxPrefetched;

    private final int prefetchThreshold;

    private final int blockSize;

    final long id;

    @Nonnull
    private final Path path;

    @Nonnull
    final Position position;

    long firstNeededBlockNumber;

    @Nonnull
    private final Position readPosition;

    private boolean readPositionValid;

    private final BlockBuffer[] queue;

    /**
     * Next index after last occupied item.
     */
    private int currentPosition;

    /**
     * Count of occupied items.
     */
    private int queueUsed;

    boolean hitEnd;

    /**
     * ledger file block number for the last occupied item.
     */
    private long blockFilePosition;

    Slot prev;

    Slot next;

    private final AtomicBoolean wakeup = new AtomicBoolean();

    private final AtomicReference<Thread> workerThread = new AtomicReference<>();

    public Slot(
            @Nonnull BlockWriterImpl blockWriter,
            int maxPrefetched,
            int blockSize,
            long id,
            @Nonnull Path path,
            @Nonnull Position position,
            long firstNeededBlockNumber
    ) {
        if (maxPrefetched < PREFETCH_FACTOR) {
            throw new IllegalArgumentException();
        }

        this.blockWriter = blockWriter;
        this.maxPrefetched = maxPrefetched;
        this.prefetchThreshold = maxPrefetched - maxPrefetched / PREFETCH_FACTOR;
        this.blockSize = blockSize;
        this.id = id;
        this.path = path;
        this.queue = new BlockBuffer[maxPrefetched];

        acquirePosition();
        try {
            this.position = position;
            this.readPosition = position.copy();
            this.firstNeededBlockNumber = firstNeededBlockNumber;
        } finally {
            releasePosition();
        }
    }

    public class Cursor implements AutoCloseable {

        private final long sequence;

        private final long offset;

        @Nonnull
        private final Thread currentThread;

        Cursor(long sequence, long offset, @Nonnull Thread currentThread) {
            logger.info("POOL: cursor opened topicId={} slotId={} available={}", blockWriter.topicId, id,
                    blockWriter.blockBufferPool.available());
            this.sequence = sequence;
            this.offset = offset;
            this.currentThread = currentThread;
        }

        void initialize() {

            final var indexBee = new ParkingBee(blockWriter.indexBloom, currentThread);

            final var blockNumber = blockWriter.getFileBlockNumber(sequence, offset, indexBee);
            logger.info("LEDGER#{}: Slot read sequence={} offset={} blockNumber={}", blockWriter.topicId, sequence,
                    offset, blockNumber);
            if (blockNumber == -1) {
                throw new IllegalArgumentException();
            }

            acquirePosition();
            try {
                if (position.compareTo(sequence, offset) > 0) {
                    throw new IllegalArgumentException();
                }

                readPosition.set(sequence, offset);
                logger.info("Initial read position: {}", readPosition);
                readPositionValid = true;
            } finally {
                releasePosition();
            }

            final int countPrefetched;
            blockWriter.acquire();
            try {
                final var delta = blockWriter.getDelta(blockNumber);
                countPrefetched = blockWriter.get(blockNumber, queue, 0, maxPrefetched, delta);
                logger.info("POOL: cursor init topicId={} slotId={} available={}", blockWriter.topicId, id,
                        blockWriter.blockBufferPool.available());
                hitEnd = delta < maxPrefetched;
            } finally {
                blockWriter.release();
            }

            currentPosition = 0;
            queueUsed = countPrefetched;
            blockFilePosition = (blockNumber + countPrefetched - 1) % blockWriter.blockFileSize;
        }

        /**
         * Get first at maximum <code>length</code> completed (and last partial) blocks which are available immediately.
         * Does not move the current block position. User not permitted to change the buffers' state or content.
         *
         * @param buffers block buffers to retrieve
         * @param start   beginning array index to fill in
         * @param length  count of array elements to fill in
         * @return actual number of elements filled, may be 0 but not more than <code>length</code>
         */
        public int next(int fromPosition, @Nonnull ByteBuffer[] buffers, int start, int length) {

            if (fromPosition < 0) {
                throw new IllegalArgumentException();
            }

            if (fromPosition > queueUsed) {
                throw new IllegalArgumentException();
            }

            if (length < 0) {
                throw new IllegalArgumentException();
            }

            if (start < 0) {
                throw new IllegalArgumentException();
            }

            if (start > buffers.length) {
                throw new IllegalArgumentException();
            }

            if (length > buffers.length - start) {
                throw new IllegalArgumentException();
            }

            final var end = Math.min(queueUsed, fromPosition + length);

            BlockBuffer lastItem = null;

            for (var i = fromPosition; i < end; i++) {
                final var item = queue[(currentPosition + i) % maxPrefetched];
                final var availableDataLength = item.availableDataLength;
                final var stableSize = item.stableSize;
                if (availableDataLength == 0 || stableSize == 0) {
                    if (lastItem != null) {
                        advance(lastItem);
                    }
                    return i - fromPosition;
                }
                buffers[start + i] = item.subscriberBuffer.slice(0, Math.min(availableDataLength, stableSize));
                if (availableDataLength != blockSize || stableSize != blockSize) {
                    advance(item);
                    return i - fromPosition + 1;
                }
                lastItem = item;
            }

            if (lastItem != null) {
                advance(lastItem);
            }
            return end - fromPosition;
        }

        /**
         * Makes a bee for waiting the block update (complete loading or data growth if block incompleted).
         *
         * @param n         block position, relative to the current block
         * @param aboveSize available data must be greater than this
         * @param hive      hive to make a bee
         * @param value     arbitrary user-supplied value
         * @return <code>null</code> if more block data available or in case of shutdown
         */
        @Nullable
        public <V> HiveBee<V, ?> next(int n, int aboveSize, @Nonnull Hive<V, ?> hive, V value) {

            if (n < 0) {
                throw new IllegalArgumentException();
            }

            if (n >= queueUsed) {
                throw new IllegalArgumentException();
            }

            final var item = queue[(currentPosition + n) % maxPrefetched];

            if (item.availableDataLength > aboveSize || blockWriter.shutdown.get()) {
                return nextStable(item, aboveSize, hive, value);
            }

            final var bee = hive.bee(item.dataBloom, value);
            bee.ready();

            if (item.availableDataLength > aboveSize || blockWriter.shutdown.get()) {
                bee.close();
                return nextStable(item, aboveSize, hive, value);
            }

            return bee;
        }

        @Nullable
        private <V> HiveBee<V, ?> nextStable(@Nonnull BlockBuffer item, int aboveSize, @Nonnull Hive<V, ?> hive,
                V value) {

            if (item.stableSize > aboveSize || blockWriter.shutdown.get()) {
                return null;
            }

            final var bee = hive.bee(item.stabBloom, value);
            bee.ready();

            if (item.stableSize > aboveSize || blockWriter.shutdown.get()) {
                bee.close();
                return null;
            }

            return bee;
        }

        /**
         * Skip N buffers startin from the current read position. Unloaded blocks cannot be skipped.
         * Incomplete (last) block cannot be skipped.
         * Do not skip blocks beyond received from {@link #next(int, ByteBuffer[], int, int)}.
         * Do not skip blocks received from
         * {@link #next(int, ByteBuffer[], int, int)} and still in use.
         */
        public void skip(int n) {

            if (n == 0) {
                return;
            }

            if (n < 0) {
                throw new IllegalArgumentException();
            }

            if (n > queueUsed) {
                throw new IllegalArgumentException();
            }

            if (availableDataLength(n - 1) < blockSize) {
                throw new IllegalArgumentException();
            }

            blockWriter.clean(queue, currentPosition, n);
            currentPosition = (currentPosition + n) % maxPrefetched;
            queueUsed -= n;

            logger.info("POOL: cursor skip topicId={} slotId={} available={}", blockWriter.topicId, id,
                    blockWriter.blockBufferPool.available());
        }

        public int prefetched() {
            return queueUsed;
        }

        public int maxPrefetch() {
            return maxPrefetched;
        }

        public int prefetchThreshold() {
            return prefetchThreshold;
        }

        public int blockSize() {
            return blockSize;
        }

        @Nullable
        public <V> HiveBee<V, ?> prefetch(@Nonnull Hive<V, ?> hive, V value) {

            if (queueUsed < prefetchThreshold) {
                rawPrefetch();
                if (queueUsed == 0) {
                    final var bee = hive.bee(blockWriter.queuePositionBloom, value);
                    bee.ready();
                    rawPrefetch();
                    if (queueUsed == 0 && !blockWriter.shutdown.get()) {
                        return bee;
                    }
                    bee.close();
                }
            }
            return null;
        }

        public void prefetch() {

            if (queueUsed < prefetchThreshold) {
                rawPrefetch();
            }
        }

        /**
         * Retrievs the available data length for the Nth block, starting from the current block position.
         *
         * @param n block position, relative to the current block
         * @return available data length, not less than zero, not more than block size. Equality to the block size
         * mean completion of the block.
         */
        public int availableDataLength(int n) {

            if (n < 0) {
                throw new IllegalArgumentException();
            }

            if (n < queueUsed) {
                final var item = queue[(currentPosition + n) % maxPrefetched];
                return item.availableDataLength;
            } else {
                return 0;
            }
        }

        public int stableDataLength(int n) {

            if (n < 0) {
                throw new IllegalArgumentException();
            }

            if (n < queueUsed) {
                final var item = queue[(currentPosition + n) % maxPrefetched];
                return item.stableSize;
            } else {
                blockWriter.acquire();
                try {
                    final var delta = blockWriter.getDelta(blockFilePosition);
                    final var shift = n - queueUsed + 1;
                    if (delta < shift) {
                        throw new IllegalArgumentException();
                    }
                    return blockWriter.getStableSize((blockFilePosition + shift) % blockWriter.blockFileSize);
                } finally {
                    blockWriter.release();
                }
            }
        }

        private void advance(@Nonnull BlockBuffer item) {

            final var buffer = item.subscriberBuffer;
            final var availableDataLength = Math.min(item.availableDataLength, item.stableSize);

            final long sequence;
            final long offset;

            var position = 0;

            while (true) {

                final var chunkLength = buffer.getShort(position + 16) & 0xFFFF;
                final var nextPosition = position + chunkLength + HEADER_SIZE + TRAILER_SIZE;

                if (availableDataLength - nextPosition < HEADER_SIZE + TRAILER_SIZE) {

                    final var rawSequence = buffer.getLong(position);

                    if (rawSequence < 0) {
                        sequence = 1 - rawSequence;
                        offset = 0;
                    } else {
                        sequence = rawSequence;
                        offset = buffer.getLong(position + 8) + chunkLength;
                    }

                    break;
                }

                position = nextPosition;
            }

            acquirePosition();
            try {
                if (readPosition.compareTo(sequence, offset) < 0) {
                    readPosition.set(sequence, offset);
                    logger.info("New read position: {}", readPosition);
                }
            } finally {
                releasePosition();
            }
        }

        @Override
        public void close() {

            blockWriter.clean(queue, currentPosition, queueUsed);
            wakeup.set(false);
            workerThread.set(null);

            acquirePosition();
            try {
                readPositionValid = false;
            } finally {
                releasePosition();
            }

            logger.info("POOL: cursor closed topicId={} slotId={} available={}", blockWriter.topicId, id,
                    blockWriter.blockBufferPool.available());
        }
    }

    @Nullable
    public Cursor cursor(long sequence, long offset) {

        if (blockWriter.shutdown.get()) {
            return null;
        }

        final var currentThread = Thread.currentThread();

        final var actualValue = workerThread.compareAndExchange(null, currentThread);
        if (actualValue != null) {
            throw new IllegalStateException(
                    "Already locked by thread: " + actualValue.getName() + " in state: " + actualValue.getState());
        }

        final var cursor = new Cursor(sequence, offset, currentThread);
        try {
            cursor.initialize();
            return cursor;
        } catch (Throwable e) {
            cursor.close();
            throw e;
        }
    }

    public boolean read(long sequence, long offset, @Nonnull ReaderCallback readerCallback) {

        if (blockWriter.shutdown.get()) {
            readerCallback.close();
            return false;
        }

        final var currentThread = Thread.currentThread();

        if (!workerThread.compareAndSet(null, currentThread)) {
            throw new IllegalStateException();
        }

        try {
            final var indexBee = new ParkingBee(blockWriter.indexBloom, currentThread);
            final var queuePositionBee = new ParkingBee(blockWriter.queuePositionBloom, currentThread);

            final var blockNumber = blockWriter.getFileBlockNumber(sequence, offset, indexBee);
            logger.info("LEDGER#{}: Slot read sequence={} offset={} blockNumber={}", blockWriter.topicId, sequence,
                    offset, blockNumber);
            if (blockNumber == -1) {
                throw new IllegalArgumentException();
            }

            acquirePosition();
            try {
                if (position.compareTo(sequence, offset) > 0) {
                    throw new IllegalArgumentException();
                }

                readPosition.set(sequence, offset);
                logger.info("Initial read position: {}", readPosition);
                readPositionValid = true;
            } finally {
                releasePosition();
            }

            final int countPrefetched;
            blockWriter.acquire();
            try {
                final var delta = blockWriter.getDelta(blockNumber);
                countPrefetched = blockWriter.get(blockNumber, queue, 0, maxPrefetched, delta);
                logger.info("POOL: cursor read topicId={} slotId={} available={}", blockWriter.topicId, id,
                        blockWriter.blockBufferPool.available());
                hitEnd = delta < maxPrefetched;
            } finally {
                blockWriter.release();
            }

            currentPosition = 0;
            var bufferPosition = 0;
            queueUsed = countPrefetched;
            blockFilePosition = (blockNumber + countPrefetched - 1) % blockWriter.blockFileSize;

            while (true) {

                if (blockWriter.shutdown.get()) {
                    readerCallback.close();
                    return false;
                }

                final var item = queue[currentPosition];
                int availableDataLength;

                if ((availableDataLength = item.availableDataLength) == bufferPosition) {
                    if (queueUsed == 1 && hitEnd && !readerCallback.end()) {
                        return true;
                    }
                    final var bee = new ParkingBee(item.dataBloom, currentThread);
                    while (true) {
                        while (wakeup.getAndSet(false)) {
                            if (!readerCallback.wakeup()) {
                                return false;
                            }
                        }
                        bee.ready();
                        if ((availableDataLength = item.availableDataLength) != bufferPosition) {
                            break;
                        }
                        if (blockWriter.shutdown.get()) {
                            readerCallback.close();
                            return false;
                        }
                        LockSupport.park();
                        if ((availableDataLength = item.availableDataLength) != bufferPosition) {
                            break;
                        }
                    }
                }

                final var buffer = item.subscriberBuffer;

                logger.info("LEDGER#{}: Slot next block sequence={} offset={} blockNumber={}", blockWriter.topicId,
                        Math.abs(buffer.getLong(0)), buffer.getLong(8), item.blockFilePosition);

                final long s;
                final long o;

                var p = bufferPosition;

                while (true) {

                    final var l = buffer.getShort(p + 16) & 0xFFFF;
                    final var p2 = p + l + HEADER_SIZE + TRAILER_SIZE;

                    if (availableDataLength - p2 < HEADER_SIZE + TRAILER_SIZE) {

                        final var v = buffer.getLong(p);

                        if (v < 0) {
                            s = 1 - v;
                            o = 0;
                        } else {
                            s = v;
                            o = buffer.getLong(p + 8) + l;
                        }

                        break;
                    }

                    p = p2;
                }

                acquirePosition();
                try {
                    if (readPosition.compareTo(s, o) < 0) {
                        readPosition.set(s, o);
                        logger.info("New read position: {}", readPosition);
                    }
                } finally {
                    releasePosition();
                }

                if (!readerCallback.next(buffer.slice(bufferPosition, availableDataLength - bufferPosition))) {
                    return false;
                }

                if (availableDataLength == blockSize) {
                    blockWriter.clean(queue, currentPosition, 1);
                    logger.info("POOL: cursor clean topicId={} slotId={} available={}", blockWriter.topicId, id,
                            blockWriter.blockBufferPool.available());
                    currentPosition = (currentPosition + 1) % maxPrefetched;
                    queueUsed--;
                    bufferPosition = 0;
                } else {
                    bufferPosition = availableDataLength;
                }

                if (queueUsed < prefetchThreshold) {
                    rawPrefetch();
                    if (queueUsed == 0) {
                        if (!readerCallback.end()) {
                            return true;
                        }
                        while (true) {
                            while (wakeup.getAndSet(false)) {
                                if (!readerCallback.wakeup()) {
                                    return false;
                                }
                            }
                            queuePositionBee.ready();
                            rawPrefetch();
                            if (queueUsed != 0) {
                                break;
                            }
                            if (blockWriter.shutdown.get()) {
                                readerCallback.close();
                                return false;
                            }
                            LockSupport.park();
                            rawPrefetch();
                            if (queueUsed != 0) {
                                break;
                            }
                        }
                    }
                }
            }
        } finally {
            blockWriter.clean(queue, currentPosition, queueUsed);
            wakeup.set(false);
            workerThread.set(null);

            acquirePosition();
            try {
                readPositionValid = false;
            } finally {
                releasePosition();
            }

            logger.info("POOL: cursor finished topicId={} slotId={} available={}", blockWriter.topicId, id,
                    blockWriter.blockBufferPool.available());
        }
    }

    private void rawPrefetch() {

        blockWriter.acquire();
        try {
            final var delta = blockWriter.getDelta(blockFilePosition);
            final var prefetchCount = (int) Math.min(delta, maxPrefetched - queueUsed);

            if (prefetchCount > 0) {
                final var countPrefetched = blockWriter.get(
                        (blockFilePosition + 1) % blockWriter.blockFileSize,
                        queue,
                        (currentPosition + queueUsed) % maxPrefetched,
                        prefetchCount,
                        delta - 1
                );
                logger.info("POOL: cursor prefetch topicId={} slotId={} available={}", blockWriter.topicId, id,
                        blockWriter.blockBufferPool.available());
                hitEnd = delta <= prefetchCount;
                queueUsed += countPrefetched;
                blockFilePosition = (blockFilePosition + countPrefetched) % blockWriter.blockFileSize;
            }
        } finally {
            blockWriter.release();
        }
    }

    public void wakeup() {

        wakeup.set(true);
        final var thread = workerThread.get();
        if (thread != null) {
            LockSupport.unpark(thread);
        }
    }

    public void consume(long sequence, long offset) {

        acquirePosition();
        try {
            final var r = position.compareTo(sequence, offset);

            if (r > 0) {
                throw new IllegalArgumentException();
            }

            if (r == 0) {
                return;
            }

            if (readPositionValid && readPosition.compareTo(sequence, offset) < 0) {
                throw new IllegalArgumentException(
                        "Invalid position {sequence=" + sequence + ", offset=" + offset + "}: must be not less than " +
                        readPosition);
            }

            final var currentThread = Thread.currentThread();
            final var indexBee = new ParkingBee(blockWriter.indexBloom, currentThread);

            final var blockNumber = blockWriter.getFileBlockNumber(sequence, offset, indexBee);
            if (blockNumber == -1) {
                throw new IllegalArgumentException("Not found sequence=" + sequence + " offset=" + offset);
            }

            blockWriter.acquireSlots();
            try {
                position.set(sequence, offset);
                writeSlot();
                blockWriter.acquire();
                try {
                    firstNeededBlockNumber = blockNumber;
                    logger.info(
                            "LEDGER#{}: Updated slot id={} sequence={} offset={} firstNeededBlockNumber={} firstNeededBlockFilePosition={}",
                            blockWriter.topicId, id, position.sequence, position.offset, firstNeededBlockNumber,
                            blockWriter.updateFirstNeededBlockFilePosition());
                } finally {
                    blockWriter.release();
                }
            } finally {
                blockWriter.releaseSlots();
            }
        } finally {
            releasePosition();
        }
    }

    private void acquirePosition() {

        while (true) {
            if (positionLock.compareAndSet(false, true)) {
                break;
            }
        }
    }

    private void releasePosition() {
        positionLock.set(false);
    }

    void writeSlot() {

        try {
            final var tmpPath = path.getParent().resolve(path.getFileName().toString() + ".tmp");
            Files.write(tmpPath, Arrays.asList(String.valueOf(position.sequence), String.valueOf(position.offset)),
                    UTF_8);
            Files.move(tmpPath, path, REPLACE_EXISTING, ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void deleteSlot() {

        try {
            Files.delete(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public long getId() {
        return id;
    }

    @Nullable
    static Slot max(@Nullable Slot head) {

        if (head == null) {
            return null;
        }

        var max = head;
        var item = head;

        while (true) {
            item = item.next;
            if (item == head) {
                break;
            }
            if (item.position.compareTo(max.position) > 0) {
                max = item;
            }
        }

        return max;
    }

    @Nullable
    static Slot min(@Nullable Slot head) {

        if (head == null) {
            return null;
        }

        var min = head;
        var item = head;

        while (true) {
            item = item.next;
            if (item == head) {
                break;
            }
            if (item.position.compareTo(min.position) < 0) {
                min = item;
            }
        }

        return min;
    }

    @Nullable
    static Slot readSlots(
            @Nonnull BlockWriterImpl blockWriter,
            int maxPrefetched,
            int blockSize,
            @Nonnull Path path
    ) throws IOException {

        final var paths = Files.list(path).filter(e -> e.getFileName().toString().endsWith(SLOT_SUFFIX)).toList();
        Slot head = null;
        final var indexBee = new ParkingBee(blockWriter.indexBloom, Thread.currentThread());
        for (final var p : paths) {
            final var name = p.getFileName().toString();
            final var id = Long.parseLong(name.substring(0, name.length() - SLOT_SUFFIX.length()));
            final var position = readSlot(p);
            final var firstNeededBlockNumber =
                    blockWriter.getFileBlockNumber(position.sequence, position.offset, indexBee);

            if (firstNeededBlockNumber == -1) {
                throw new IllegalArgumentException(
                        "Not found sequence=" + position.sequence + " offset=" + position.offset);
            }

            final var slot = new Slot(blockWriter, maxPrefetched, blockSize, id, p, position, firstNeededBlockNumber);
            if (head == null) {
                head = slot;
                slot.prev = slot;
                slot.next = slot;
            } else {
                final var tail = head.prev;
                slot.prev = tail;
                slot.next = head;
                head.prev = slot;
                tail.next = slot;
            }
        }
        return head;
    }

    @Nonnull
    private static Position readSlot(@Nonnull Path path) throws IOException {

        final var lines = Files.readAllLines(path, UTF_8);
        final var sequence = Long.parseLong(lines.get(0));
        final var offset = Long.parseLong(lines.get(1));
        return new Position(sequence, offset);
    }

    Map<String, Object> getInfo() {

        final var map = new HashMap<String, Object>();
        acquirePosition();
        try {
            map.put("sequence", position.sequence);
            map.put("offset", position.offset);
            map.put("firstNeededBlockNumber", firstNeededBlockNumber);
        } finally {
            releasePosition();
        }
        return map;
    }
}
