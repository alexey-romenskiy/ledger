package codes.writeonce.ledger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

import static codes.writeonce.ledger.MessageWriter.HEADER_SIZE;
import static codes.writeonce.ledger.MessageWriter.MAX_CHUNK_DATA_SIZE;
import static codes.writeonce.ledger.MessageWriter.TRAILER_SIZE;
import static com.sun.nio.file.ExtendedOpenOption.DIRECT;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.DSYNC;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;

public class LedgerBuilder {

    private static final int MAX_PID_LENGTH = 1000;

    private static final AtomicLong WRITER_THREAD_SEQUENCE = new AtomicLong();

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final long topicId;

    private long defaultSequence = 1;
    private long defaultOffset = 0;
    private int maxPrefetched = 0x1000;
    private int maxCached = 0x4000;
    private int poolBlockCount = 0x40000;
    private int blockSize = 0x1000;
    private long ledgerBlockCount = 0x4000000;
    private Path ledgerPath;
    private Path indexPath;
    private Path lockPath;
    private Path slotDirPath;
    private boolean forceRebuild;
    private boolean autoCreateLedgerFile;
    private LedgerMetrics ledgerMetrics = EmptyLedgerMetrics.INSTANCE;

    @Nonnull
    public Ledger build() throws IOException, LedgerException {

        requireNonNull(ledgerPath);
        requireNonNull(indexPath);
        requireNonNull(lockPath);

        FileChannel lockChannel = null;
        FileLock fileLock = null;
        try {
            try {
                lockChannel = FileChannel.open(lockPath, CREATE_NEW, WRITE);
                fileLock = lockChannel.tryLock(0, Long.MAX_VALUE, false);
                if (fileLock == null) {
                    throw new LedgerException("Lock file is occupied by another process");
                }
                writePid(lockChannel);
                return openStorage(lockPath, lockChannel, fileLock, false, forceRebuild);
            } catch (FileAlreadyExistsException e) {
                lockChannel = FileChannel.open(lockPath, READ, WRITE);
                fileLock = lockChannel.tryLock(0, Long.MAX_VALUE, false);
                if (fileLock == null) {
                    throw new LedgerException("Lock file is occupied by another process");
                }
                final var pid = readPid(lockChannel);
                if (isProcessAlive(pid)) {
                    throw new LedgerException("Lock file is owned by the live process with PID: " + pid);
                }
                lockChannel.truncate(0);
                writePid(lockChannel);
                return openStorage(lockPath, lockChannel, fileLock, true, forceRebuild);
            }
        } catch (Throwable e) {
            // NoSuchFileException for incorrect file path

            if (fileLock != null) {
                fileLock.close();
            }

            if (lockChannel != null) {
                lockChannel.close();
            }

            throw e;
        }
    }

    @Nonnull
    private Ledger openStorage(
            @Nonnull Path lockPath,
            @Nonnull FileChannel lockChannel,
            @Nonnull FileLock fileLock,
            boolean unclean,
            boolean forceRebuild
    ) throws IOException, LedgerException {

        if (!Files.exists(ledgerPath)) {
            if (autoCreateLedgerFile) {
                logger.info("LEDGER#{}: Ledger's data file not found, creating", topicId);
                try (var channel = FileChannel.open(ledgerPath, WRITE, CREATE_NEW)) {
                    final var size = (long) blockSize * ledgerBlockCount;
                    if (channel.size() < size) {
                        channel.write(ByteBuffer.allocate(1), size - 1);
                    }
                }
                Files.deleteIfExists(indexPath);
            } else {
                throw new LedgerException("Ledger data file does not exist: " + ledgerPath);
            }
        } else if (unclean) {
            logger.info("LEDGER#{}: Unclean shutdown encountered, rebuilding the ledger's index", topicId);
            rebuildIndex();
        } else if (forceRebuild) {
            logger.info("LEDGER#{}: Enforced rebuilding the ledger's index", topicId);
            rebuildIndex();
        } else if (!Files.exists(indexPath)) {
            logger.info("LEDGER#{}: Ledger's index not found, rebuilding", topicId);
            rebuildIndex();
        }

        FileChannel indexChannel = null;
        AsynchronousFileChannel channel = null;
        try {
            indexChannel = FileChannel.open(indexPath, READ, WRITE, CREATE);
            final var options = new HashSet<OpenOption>(Arrays.asList(READ, WRITE));
            if (getOption("codes.writeonce.ledger.Ledger.DSYNC", true)) {
                options.add(DSYNC);
            }
            if (getOption("codes.writeonce.ledger.Ledger.DIRECT", true)) {
                options.add(DIRECT);
            }
            final ExecutorService executor;
            if (getOption("codes.writeonce.ledger.Ledger.executor.enabled", false)) {
                executor = Executors.newFixedThreadPool(
                        Integer.getInteger("codes.writeonce.ledger.Ledger.executor.threads", 256),
                        r -> new Thread(r, "ldgr#" + topicId + "-" + WRITER_THREAD_SEQUENCE.incrementAndGet())
                );
            } else {
                executor = null;
            }
            channel = AsynchronousFileChannel.open(ledgerPath, options, executor);

            final var zeroes = new byte[blockSize];
            final var blockBufferPool = new BufferPool(blockSize, poolBlockCount, zeroes);
            final var buffer = blockBufferPool.acquire();

            final long readyMessageSequence;
            final long readyMessageOffset;
            final long blockFilePosition;
            final long lastIndexedFileBlock;
            final int lastIndexedDistance;
            final long lastBlockSequence;
            final long lastBlockOffset;
            int position;

            final var index = new Index(topicId, blockSize, ledgerBlockCount, indexChannel);
            final var count = index.getFilledBlockCount();
            if (count == 0) {
                readyMessageSequence = defaultSequence;
                readyMessageOffset = defaultOffset;
                blockFilePosition = 0;
                lastIndexedFileBlock = -1;
                lastIndexedDistance = 1;
                position = 0;
                lastBlockSequence = readyMessageSequence;
                lastBlockOffset = readyMessageOffset;
            } else {
                final var first = index.getFirstFilledBlock();
                final var bfp = (first + count - 1) % ledgerBlockCount;

                try {
                    channel.read(buffer.writeBuffer, bfp * blockSize).get();
                } catch (ExecutionException e) {
                    final var cause = e.getCause();
                    if (cause instanceof IOException e2) {
                        throw e2;
                    } else {
                        throw new LedgerException("IO operation failed", cause);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new LedgerException("IO operation interrupted", e);
                }

                if (buffer.writeBuffer.hasRemaining()) {
                    throw new LedgerException("Index file is corrupted");
                }

                final var lastBlockPosition = new Position(1, 0);
                index.get(bfp, lastBlockPosition);

                buffer.writeBuffer.clear();
                final var receiver = new MessageReceiverImpl(lastBlockPosition.sequence, lastBlockPosition.offset);
                final var messageReader = new MessageReader();
                try {
                    messageReader.next(buffer.writeBuffer, receiver);
                } catch (IllegalArgumentException | IndexOutOfBoundsException ignore) {
                    // empty
                }
                buffer.writeBuffer.clear();

                position = receiver.lastPosition;
                if (position > blockSize - (HEADER_SIZE + TRAILER_SIZE)) {
                    blockFilePosition = (bfp + 1) % ledgerBlockCount;
                    lastIndexedDistance = 1;
                    buffer.writeBuffer.put(0, zeroes);
                    index.remove(blockFilePosition);
                    position = 0;
                    lastBlockSequence = receiver.lastSequence;
                    lastBlockOffset = receiver.lastOffset;
                } else {
                    blockFilePosition = bfp;
                    lastIndexedDistance = 0;
                    buffer.writeBuffer.put(position, new byte[blockSize - position]);
                    lastBlockSequence = Math.abs(buffer.writeBuffer.getLong(0));
                    lastBlockOffset = buffer.writeBuffer.getLong(8);
                }

                lastIndexedFileBlock = bfp;

                readyMessageSequence = receiver.lastSequence;
                readyMessageOffset = receiver.lastOffset;
            }

            logger.info("LEDGER#{}: opened, sequence={} offset={} blockPosition={} position={}", topicId,
                    readyMessageSequence, readyMessageOffset, blockFilePosition, position);

            final var blockWriter = new BlockWriterImpl(
                    topicId,
                    blockBufferPool,
                    blockSize,
                    maxCached,
                    blockFilePosition,
                    ledgerBlockCount,
                    buffer,
                    position,
                    channel,
                    index,
                    lastIndexedFileBlock,
                    lastIndexedDistance,
                    maxPrefetched,
                    slotDirPath,
                    readyMessageSequence,
                    readyMessageOffset,
                    lastBlockSequence,
                    lastBlockOffset,
                    zeroes,
                    executor,
                    ledgerMetrics
            );

            final var messageWriter = new MessageWriter(
                    topicId,
                    blockSize,
                    blockWriter,
                    buffer.writeBuffer,
                    position,
                    MAX_CHUNK_DATA_SIZE,
                    readyMessageSequence,
                    readyMessageOffset
            );

            return new Ledger(topicId, messageWriter, blockWriter, lockPath, lockChannel, fileLock);
        } catch (Throwable e) {
            if (indexChannel != null) {
                indexChannel.close();
            }
            if (channel != null) {
                channel.close();
            }
            throw e;
        }
    }

    private static boolean getOption(
            @Nonnull String name,
            @SuppressWarnings("SameParameterValue") boolean defaultValue
    ) {
        return ofNullable(System.getProperty(name)).map(Boolean::parseBoolean).orElse(defaultValue);
    }

    private static class ScanEntry {

        @Nonnull
        private final ByteBuffer byteBuffer;

        private CompletableFuture<Void> future;

        public ScanEntry(@Nonnull ByteBuffer byteBuffer) {
            this.byteBuffer = byteBuffer;
        }
    }

    private void rebuildIndex() throws IOException {

        final var indexBuffer = getIndexBuffer();
        final var blockItemCount = blockSize / 16;
        final var levelOffsets = Index.getLevelOffsets(ledgerBlockCount, blockItemCount, blockSize);
        final var pool = getPool();

        final var handler = new CompletionHandler<Integer, ScanEntry>() {
            @Override
            public void completed(Integer result, ScanEntry entry) {
                entry.future.complete(null);
            }

            @Override
            public void failed(Throwable throwable, ScanEntry entry) {
                entry.future.completeExceptionally(throwable);
            }
        };

        final var checksum = new CRC32();

        long firstBlock = -1;
        long firstSequence = 0;
        long firstOffset = 0;
        long lastSequence = 0;
        long lastOffset = 0;
        var lastFinished = false;
        long wrapBlock = -1;
        long wrapSequence = 0;
        long wrapOffset = 0;
        long firstGapBlock = -1;
        long wrapGapBlock = -1;
        long wrapGapSequence = 0;
        long wrapGapOffset = 0;
        long firstGapSequence = 0;
        long firstGapOffset = 0;
        var lastGap = false;

        try (var ledgerChannel = AsynchronousFileChannel.open(ledgerPath, READ, WRITE, DSYNC, DIRECT)) {

            final var prefetchCount = Math.min(ledgerBlockCount, maxPrefetched);
            for (long i = 0; i < prefetchCount; i++) {
                read(ledgerChannel, handler, i, pool);
            }

            for (long i = 0; i < ledgerBlockCount; i++) {
                final var entry = getPrefetched(pool, i);

                final var byteBuffer = entry.byteBuffer;
                byteBuffer.clear();
                final var limit = byteBuffer.limit();
                var position = byteBuffer.position();
                var remaining = limit - position;

                while (remaining >= HEADER_SIZE + TRAILER_SIZE) {
                    final var s = byteBuffer.getLong(position);
                    if (s == 0 || s == Long.MIN_VALUE) {
                        break;
                    }
                    final var offset = byteBuffer.getLong(position + 8);
                    final var length = byteBuffer.getShort(position + 16) & 0xFFFF;
                    final var dataPosition = position + HEADER_SIZE;
                    final var checksumPosition = dataPosition + length;
                    if (checksumPosition > byteBuffer.limit() - 4) {
                        throw new IllegalArgumentException("Incorrect chunk length=" + length +
                                                           " dataPosition=" + dataPosition +
                                                           " limit=" + byteBuffer.limit());
                    }
                    final var storedChecksum = byteBuffer.getInt(checksumPosition);
                    byteBuffer.limit(checksumPosition);
                    byteBuffer.position(position);
                    checksum.reset();
                    checksum.update(byteBuffer);
                    if (storedChecksum != (int) checksum.getValue()) {
                        break;
                    }
                    byteBuffer.limit(limit);
                    final var sequence = Math.abs(s);

                    if (position == 0) {
                        if (firstBlock == -1) {
                            firstBlock = i;
                            firstSequence = sequence;
                            firstOffset = offset;
                        } else if (sequence < lastSequence || sequence == lastSequence && offset < lastOffset) {
                            // wrap found
                            if (wrapBlock == -1 ||
                                sequence > wrapSequence ||
                                sequence == wrapSequence && offset > wrapOffset) {
                                // first or better wrap
                                wrapBlock = i;
                                wrapGapBlock = -1;
                                wrapSequence = sequence;
                                wrapOffset = offset;
                                wrapGapSequence = lastSequence;
                                wrapGapOffset = lastOffset;
                            }
                        } else if (!lastGap) {
                            if (lastSequence == sequence) {
                                if (lastOffset != offset) {
                                    throw new IllegalArgumentException();
                                }
                            } else if (lastFinished || lastSequence + 1 != sequence) {
                                throw new IllegalArgumentException();
                            }
                        }
                        putIndex(levelOffsets, blockItemCount, indexBuffer, i, sequence, offset);
                    } else {
                        if (lastSequence == sequence) {
                            if (lastOffset != offset) {
                                throw new IllegalArgumentException();
                            }
                        } else if (lastFinished || lastSequence + 1 != sequence) {
                            throw new IllegalArgumentException();
                        }
                    }

                    if (s < 0) {
                        lastOffset = 0;
                        lastSequence = 1 - s;
                        lastFinished = true;
                    } else {
                        lastOffset = offset + length;
                        lastSequence = s;
                        lastFinished = false;
                    }

                    final var chunkSize = HEADER_SIZE + TRAILER_SIZE + length;
                    position += chunkSize;
                    remaining -= chunkSize;
                }

                if (remaining >= HEADER_SIZE + TRAILER_SIZE) {
                    lastGap = true;
                    final var gapBlock = position == 0 ? i : (i + 1) % ledgerBlockCount;
                    if (firstBlock != -1 && firstGapBlock == -1) {
                        firstGapBlock = gapBlock;
                        firstGapSequence = lastSequence;
                        firstGapOffset = lastOffset;
                    }
                    if (wrapBlock != -1 && wrapGapBlock == -1) {
                        wrapGapBlock = gapBlock;
                        wrapGapSequence = lastSequence;
                        wrapGapOffset = lastOffset;
                    }
                } else {
                    lastGap = false;
                }

                final var prefetchIndex = i + maxPrefetched;
                if (prefetchIndex < ledgerBlockCount) {
                    read(ledgerChannel, handler, prefetchIndex, pool);
                }
            }
        }

        if (firstBlock != -1) {
            if (firstSequence < lastSequence || firstSequence == lastSequence && firstOffset < lastOffset) {
                // wrap found
                if (wrapBlock == -1 ||
                    firstSequence > wrapSequence ||
                    firstSequence == wrapSequence && firstOffset > wrapOffset) {
                    // first or better wrap
                    wrapBlock = firstBlock;
                    wrapGapBlock = firstGapBlock;
                    wrapSequence = firstSequence;
                    wrapOffset = firstOffset;
                    wrapGapSequence = firstGapSequence;
                    wrapGapOffset = firstGapOffset;
                }
            }
            if (wrapBlock != -1) {
                if (wrapGapBlock == -1) {
                    if (firstBlock == 0) {
                        if (lastSequence == firstSequence) {
                            if (lastOffset != firstOffset) {
                                throw new IllegalArgumentException();
                            }
                        } else if (lastFinished || lastSequence + 1 != firstSequence) {
                            throw new IllegalArgumentException();
                        }
                        if (firstGapBlock == -1) {
                            wrapGapBlock = wrapBlock;
                        } else {
                            wrapGapBlock = firstGapBlock;
                            wrapGapSequence = firstGapSequence;
                            wrapGapOffset = firstGapOffset;
                        }
                    } else {
                        wrapGapBlock = 0;
                        wrapGapSequence = lastSequence;
                        wrapGapOffset = lastOffset;
                    }
                }
                invalidate(levelOffsets, blockItemCount, indexBuffer, wrapBlock, wrapGapBlock);
                logger.info(
                        "LEDGER#{}: Recovered index from block {} sequence {} offset {} to block {} sequence {} offset {}",
                        topicId, wrapBlock, wrapSequence, wrapOffset, wrapGapBlock, wrapGapSequence, wrapGapOffset);
            } else {
                clear(levelOffsets, blockItemCount, indexBuffer);
                logger.info("LEDGER#{}: Recovered index contained dummy content", topicId);
            }
        } else {
            clear(levelOffsets, blockItemCount, indexBuffer);
            logger.info("LEDGER#{}: Recovered index contained no content", topicId);
        }

        logger.info("LEDGER#{}: Index rebuilding completed", topicId);
    }

    private void invalidate(
            @Nonnull int[] levelOffsets,
            int blockItemCount,
            @Nonnull ByteBuffer buffer,
            long wrapBlock,
            long wrapGapBlock
    ) {
        final var count = wrapGapBlock - wrapBlock;
        buffer.putInt(0, (int) wrapBlock);
        buffer.putInt(4, (int) (count > 0 ? count : ledgerBlockCount + count));
        for (long i = wrapGapBlock; i != wrapBlock; i = (i + 1) % ledgerBlockCount) {
            putIndex(levelOffsets, blockItemCount, buffer, i, 0, 0);
        }
    }

    private void clear(
            @Nonnull int[] levelOffsets,
            int blockItemCount,
            @Nonnull ByteBuffer buffer
    ) {
        buffer.putInt(0, 0);
        buffer.putInt(4, 0);
        for (long i = 0; i != ledgerBlockCount; i++) {
            putIndex(levelOffsets, blockItemCount, buffer, i, 0, 0);
        }
    }

    public void putIndex(
            @Nonnull int[] levelOffsets,
            int blockItemCount,
            @Nonnull ByteBuffer buffer,
            long blockNumber,
            long sequence,
            long offset
    ) {
        int level = levelOffsets.length;
        int n = (int) blockNumber;

        while (true) {
            level--;
            final var base = levelOffsets[level];
            buffer.putLong(base + n * 16, sequence);
            buffer.putLong(base + n * 16 + 8, offset);
            if (n % blockItemCount != 0) {
                break;
            }
            if (level == 0) {
                break;
            }
            n /= blockItemCount;
        }
    }

    @Nonnull
    private ScanEntry getPrefetched(ScanEntry[] pool, long i) throws IOException {

        final var entry = getEntry(pool, i);
        try {
            entry.future.get();
            return entry;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
    }

    @Nonnull
    private ScanEntry getEntry(ScanEntry[] pool, long i) {
        return pool[(int) (i % maxPrefetched)];
    }

    private void read(
            @Nonnull AsynchronousFileChannel ledgerChannel,
            @Nonnull CompletionHandler<Integer, ScanEntry> handler,
            long i,
            @Nonnull ScanEntry[] pool
    ) {
        final var entry = getEntry(pool, i);
        entry.future = new CompletableFuture<>();
        final var byteBuffer = entry.byteBuffer;
        byteBuffer.clear();
        ledgerChannel.read(byteBuffer, i * blockSize, entry, handler);
    }

    @Nonnull
    private MappedByteBuffer getIndexBuffer() throws IOException {

        final var size = Index.getSize(blockSize, ledgerBlockCount);
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException();
        }

        try (var indexChannel = FileChannel.open(indexPath, READ, WRITE, CREATE, TRUNCATE_EXISTING)) {
            if (indexChannel.size() < size) {
                indexChannel.write(ByteBuffer.allocate(1), size - 1);
            }
            return indexChannel.map(READ_WRITE, 0, size);
        }
    }

    @Nonnull
    private ScanEntry[] getPool() {

        final var wholeBuffer = ByteBuffer.allocateDirect(blockSize * (maxPrefetched + 1) - 1);
        final var addressModulus = wholeBuffer.alignmentOffset(0, blockSize);
        final var alignedPosition = addressModulus > 0 ? blockSize - addressModulus : 0;

        final var pool = new ScanEntry[maxPrefetched];

        for (int i = 0; i < maxPrefetched; i++) {
            final var position = alignedPosition + i * blockSize;
            pool[i] = new ScanEntry(wholeBuffer.slice(position, blockSize));
        }
        return pool;
    }

    private static boolean isProcessAlive(long pid) {
        return ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false);
    }

    private static long readPid(@Nonnull FileChannel lockChannel) throws IOException, LedgerException {

        final var size = lockChannel.size();

        if (size == 0) {
            throw new LedgerException("Lock file is empty");
        }

        if (size > MAX_PID_LENGTH) {
            throw new LedgerException("Lock file is too large");
        }

        final var byteBuffer = ByteBuffer.allocate((int) size);
        lockChannel.read(byteBuffer, 0);
        byteBuffer.flip();
        final var bytes = new byte[byteBuffer.limit()];
        byteBuffer.get(bytes);
        try {
            return Long.parseLong(new String(bytes, UTF_8));
        } catch (NumberFormatException e) {
            throw new LedgerException("Lock file is contains unparseable PID");
        }
    }

    private static void writePid(@Nonnull FileChannel lockChannel) throws IOException {

        lockChannel.write(ByteBuffer.wrap(String.valueOf(ProcessHandle.current().pid()).getBytes(UTF_8)), 0);
        lockChannel.force(true);
    }

    @SuppressWarnings("UnusedReturnValue")
    public LedgerBuilder withPoolBlockCount(int poolBlockCount) {
        this.poolBlockCount = poolBlockCount;
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public LedgerBuilder withMaxPrefetched(int maxPrefetched) {
        this.maxPrefetched = maxPrefetched;
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public LedgerBuilder withMaxCached(int maxCached) {
        this.maxCached = maxCached;
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public LedgerBuilder withBlockSize(int blockFileSize) {
        this.blockSize = blockFileSize;
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public LedgerBuilder withLedgerBlockCount(long ledgerBlockCount) {
        this.ledgerBlockCount = ledgerBlockCount;
        return this;
    }

    public LedgerBuilder withLedgerPath(@Nonnull Path ledgerPath) {
        this.ledgerPath = ledgerPath;
        return this;
    }

    public LedgerBuilder withIndexPath(@Nonnull Path indexPath) {
        this.indexPath = indexPath;
        return this;
    }

    public LedgerBuilder withLockPath(@Nonnull Path lockPath) {
        this.lockPath = lockPath;
        return this;
    }

    public LedgerBuilder withSlotDirPath(Path slotDirPath) {
        this.slotDirPath = slotDirPath;
        return this;
    }

    public LedgerBuilder withForceRebuild(boolean forceRebuild) {
        this.forceRebuild = forceRebuild;
        return this;
    }

    public LedgerBuilder withAutoCreateLedgerFile(boolean autoCreateLedgerFile) {
        this.autoCreateLedgerFile = autoCreateLedgerFile;
        return this;
    }

    public LedgerBuilder withPosition(long messageSequence, long accumulatedMessageLength) {
        this.defaultSequence = messageSequence;
        this.defaultOffset = accumulatedMessageLength;
        return this;
    }

    public LedgerBuilder withPositionSequence(long messageSequence) {
        this.defaultSequence = messageSequence;
        return this;
    }

    public LedgerBuilder withPositionOffset(long accumulatedMessageLength) {
        this.defaultOffset = accumulatedMessageLength;
        return this;
    }

    public LedgerBuilder withLedgerMetrics(@Nonnull LedgerMetrics ledgerMetrics) {
        this.ledgerMetrics = requireNonNull(ledgerMetrics);
        return this;
    }

    @Nonnull
    public static LedgerBuilder newBuilder(long topicId, @Nonnull Path directoryPath) {

        return new LedgerBuilder(topicId)
                .withLedgerPath(directoryPath.resolve("data.bin"))
                .withIndexPath(directoryPath.resolve("index.bin"))
                .withLockPath(directoryPath.resolve("lock.pid"))
                .withSlotDirPath(directoryPath);
    }

    @SuppressWarnings("unused")
    @Nonnull
    public static LedgerBuilder newBuilder(
            long topicId,
            @Nonnull Path ledgerPath,
            @Nonnull Path indexPath,
            @Nonnull Path lockPath,
            @Nonnull Path slotDirPath
    ) {
        return new LedgerBuilder(topicId)
                .withLedgerPath(ledgerPath)
                .withIndexPath(indexPath)
                .withLockPath(lockPath)
                .withSlotDirPath(slotDirPath);
    }

    @SuppressWarnings("unused")
    @Nonnull
    public static LedgerBuilder newBuilder(long topicId, @Nonnull Properties p, @Nonnull Path basePath) {

        final var builder = new LedgerBuilder(topicId);

        builder.withLedgerPath(
                ofNullable(p.getProperty("ledgerPath")).map(Path::of).orElse(basePath.resolve("data.bin")));
        builder.withIndexPath(
                ofNullable(p.getProperty("indexPath")).map(Path::of).orElse(basePath.resolve("index.bin")));
        builder.withLockPath(ofNullable(p.getProperty("lockPath")).map(Path::of).orElse(basePath.resolve("lock.pid")));
        builder.withSlotDirPath(ofNullable(p.getProperty("slotDirPath")).map(Path::of).orElse(basePath));

        ofNullable(p.getProperty("autoCreateLedgerFile")).ifPresent(
                v -> builder.withAutoCreateLedgerFile(Boolean.parseBoolean(v)));
        ofNullable(p.getProperty("defaultSequence")).ifPresent(v -> builder.withPositionSequence(Long.parseLong(v)));
        ofNullable(p.getProperty("defaultOffset")).ifPresent(v -> builder.withPositionOffset(Long.parseLong(v)));
        ofNullable(p.getProperty("forceRebuild")).ifPresent(v -> builder.withForceRebuild(Boolean.parseBoolean(v)));
        ofNullable(p.getProperty("maxPrefetched")).ifPresent(v -> builder.withMaxPrefetched(Integer.parseInt(v)));
        ofNullable(p.getProperty("maxCached")).ifPresent(v -> builder.withMaxCached(Integer.parseInt(v)));
        ofNullable(p.getProperty("poolBlockCount")).ifPresent(v -> builder.withPoolBlockCount(Integer.parseInt(v)));
        ofNullable(p.getProperty("blockSize")).ifPresent(v -> builder.withBlockSize(Integer.parseInt(v)));
        ofNullable(p.getProperty("ledgerBlockCount")).ifPresent(v -> builder.withLedgerBlockCount(Long.parseLong(v)));

        return builder;
    }

    private LedgerBuilder(long topicId) {
        this.topicId = topicId;
    }

    private static class MessageReceiverImpl implements MessageReceiver {

        private int lastPosition;

        private long lastSequence;

        private long lastOffset;

        public MessageReceiverImpl(long lastSequence, long lastOffset) {
            this.lastSequence = lastSequence;
            this.lastOffset = lastOffset;
        }

        @Override
        public void next(long sequence, long offset, boolean last, @Nonnull ByteBuffer byteBuffer) {

            if (sequence <= 0) {
                throw new IllegalArgumentException();
            }

            if (offset < 0) {
                throw new IllegalArgumentException();
            }

            if (sequence == lastSequence) {
                if (offset != lastOffset) {
                    throw new IllegalArgumentException();
                }
            } else if (sequence > lastSequence) {
                if (offset != 0) {
                    throw new IllegalArgumentException();
                }
            } else {
                throw new IllegalArgumentException();
            }

            if (last) {
                lastSequence = sequence + 1;
                lastOffset = 0;
            } else {
                lastSequence = sequence;
                lastOffset = offset + byteBuffer.remaining();
            }

            final var limit = byteBuffer.limit();
            lastPosition = limit + TRAILER_SIZE;
            byteBuffer.position(limit);
        }
    }
}
