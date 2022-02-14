package codes.writeonce.ledger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.util.function.Supplier;

import static codes.writeonce.ledger.MessageWriter.HEADER_SIZE;
import static codes.writeonce.ledger.MessageWriter.TRAILER_SIZE;
import static com.sun.nio.file.ExtendedOpenOption.DIRECT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.DSYNC;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.requireNonNull;

public class LedgerBuilder {

    private static final int MAX_PID_LENGTH = 1000;

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private int maxPrefetched = 0x1000;
    private int maxCached = 0x4000;
    private int poolBlockCount = 0x40000;
    private int blockSize = 0x1000;
    private long ledgerBlockCount = 0x4000000;
    private Path ledgerPath;
    private Path indexPath;
    private Path lockPath;
    private Path slotDirPath;
    private final Supplier<Pool<BlockBuffer>> blockBufferPoolFactory = () -> new BufferPool(blockSize, poolBlockCount);

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
                return openStorage(lockChannel, fileLock, false);
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
                return openStorage(lockChannel, fileLock, true);
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
            @Nonnull FileChannel lockChannel,
            @Nonnull FileLock fileLock,
            boolean unclean
    ) throws IOException, LedgerException {

        if (unclean) {
            logger.warn("Unclean shutdown encountered, rebuilding the ledger's index");
            rebuildIndex();
        }

        FileChannel indexChannel = null;
        AsynchronousFileChannel ledgerChannel = null;
        try {
            indexChannel = FileChannel.open(indexPath, READ, WRITE);
            ledgerChannel = AsynchronousFileChannel.open(ledgerPath, DSYNC, READ, WRITE, DIRECT);

            final var pool = blockBufferPoolFactory.get();
            final var buffer = pool.acquire();

            final long sequence;
            final long length;
            final boolean finished;
            long blockFilePosition;
            int position;

            final var index = new Index(blockSize, ledgerBlockCount, indexChannel);
            final var count = index.getFilledBlockCount();
            if (count == 0) {
                sequence = 0;
                length = 0;
                finished = true;
                blockFilePosition = 0;
                position = 0;
            } else {
                final var first = index.getFirstFilledBlock();
                blockFilePosition = (first + count - 1) % ledgerBlockCount;

                ledgerChannel.read(buffer.writeBuffer, blockFilePosition * blockSize);
                if (buffer.writeBuffer.hasRemaining()) {
                    throw new LedgerException("Index file is corrupted");
                }
                buffer.writeBuffer.clear();
                final var receiver = new MessageReceiverImpl();
                final var messageReader = new MessageReader(blockSize);
                try {
                    messageReader.next(buffer.writeBuffer, receiver);
                } catch (IllegalArgumentException ignore) {
                    // empty
                }

                position = receiver.position;
                if (position > blockSize - (HEADER_SIZE + TRAILER_SIZE)) {
                    buffer.writeBuffer.put(0, new byte[blockSize]);
                    blockFilePosition = (blockFilePosition + 1) % ledgerBlockCount;
                    index.remove(blockFilePosition);
                    position = 0;
                } else {
                    buffer.writeBuffer.put(position, new byte[blockSize - position]);
                }

                sequence = receiver.sequence;
                length = receiver.length;
                finished = receiver.last;
            }

            return new Ledger(pool, blockSize, maxCached, blockFilePosition, ledgerBlockCount, ledgerChannel, buffer,
                    position, sequence, length, finished, index, slotDirPath, maxPrefetched);
        } catch (Throwable e) {
            if (indexChannel != null) {
                indexChannel.close();
            }
            if (ledgerChannel != null) {
                ledgerChannel.close();
            }
            throw e;
        }
    }

    private void rebuildIndex() throws IOException {

//        final var indexChannel = FileChannel.open(indexPath, READ, WRITE, CREATE, TRUNCATE_EXISTING);
//        final var ledgerChannel = FileChannel.open(ledgerPath, READ, WRITE);
        throw new UnsupportedOperationException();// TODO:
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

    public LedgerBuilder withPoolBlockCount(int poolBlockCount) {
        this.poolBlockCount = poolBlockCount;
        return this;
    }

    public LedgerBuilder withMaxCached(int maxCached) {
        this.maxCached = maxCached;
        return this;
    }

    public LedgerBuilder withBlockSize(int blockFileSize) {
        this.blockSize = blockFileSize;
        return this;
    }

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

    @Nonnull
    public static LedgerBuilder newBuilder(@Nonnull Path directoryPath) {

        return new LedgerBuilder()
                .withLedgerPath(directoryPath.resolve("data.bin"))
                .withIndexPath(directoryPath.resolve("index.bin"))
                .withLockPath(directoryPath.resolve("lock.pid"))
                .withSlotDirPath(directoryPath);
    }

    @Nonnull
    public static LedgerBuilder newBuilder(
            @Nonnull Path ledgerPath,
            @Nonnull Path indexPath,
            @Nonnull Path lockPath,
            @Nonnull Path slotDirPath
    ) {
        return new LedgerBuilder()
                .withLedgerPath(ledgerPath)
                .withIndexPath(indexPath)
                .withLockPath(lockPath)
                .withSlotDirPath(slotDirPath);
    }

    private LedgerBuilder() {
        // empty
    }

    private static class MessageReceiverImpl implements MessageReceiver {

        private int position;

        private long sequence;

        private long length;

        private boolean last = true;

        @Override
        public void next(long sequence, long offset, boolean last, @Nonnull ByteBuffer byteBuffer) {
            this.sequence = sequence;
            this.length = offset + byteBuffer.remaining();
            this.last = last;
            final var limit = byteBuffer.limit();
            this.position = limit + TRAILER_SIZE;
            byteBuffer.position(limit);
        }
    }
}
