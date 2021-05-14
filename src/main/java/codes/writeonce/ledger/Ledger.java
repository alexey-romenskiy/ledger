package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class Ledger implements AutoCloseable {

    private static final int MAX_PID_LENGTH = 1000;
    @Nonnull
    private final Path ledgerPath;

    private final long ledgerByteOffset;

    private final int ledgerBlockSizeBytes;

    private final long ledgerBlockCount;

    @Nonnull
    private final Path indexPath;

    private final long indexByteOffset;

    private final int indexBlockSizeBytes;

    private final long indexBlockCount;

    @Nonnull
    private final Path lockPath;

    private FileChannel lockChannel;

    private FileLock fileLock;

    private boolean opened;

    public Ledger(
            @Nonnull Path ledgerPath,
            long ledgerByteOffset,
            int ledgerBlockSizeBytes,
            long ledgerBlockCount,
            @Nonnull Path indexPath,
            long indexByteOffset,
            int indexBlockSizeBytes,
            long indexBlockCount,
            @Nonnull Path lockPath
    ) {
        this.ledgerPath = ledgerPath;
        this.ledgerByteOffset = ledgerByteOffset;
        this.ledgerBlockSizeBytes = ledgerBlockSizeBytes;
        this.ledgerBlockCount = ledgerBlockCount;
        this.indexPath = indexPath;
        this.indexByteOffset = indexByteOffset;
        this.indexBlockSizeBytes = indexBlockSizeBytes;
        this.indexBlockCount = indexBlockCount;
        this.lockPath = lockPath;
    }

    public void open() throws LedgerException {

        try {
            try {
                lockChannel = FileChannel.open(lockPath, CREATE_NEW, WRITE);
                fileLock = lockChannel.tryLock(0, Long.MAX_VALUE, false);
                if (fileLock == null) {
                    throw new LedgerException(); // occupied by another process
                }
                writePid(lockChannel);
                openStorage();
            } catch (FileAlreadyExistsException e) {
                try {
                    lockChannel = FileChannel.open(lockPath, READ, WRITE);
                    fileLock = lockChannel.tryLock(0, Long.MAX_VALUE, false);
                    if (fileLock == null) {
                        throw new LedgerException(); // occupied by another process
                    }
                    if (isProcessAlive(readPid(lockChannel))) {
                        throw new LedgerException();
                    }
                    lockChannel.truncate(0);
                    writePid(lockChannel);
                    rebuildIndex();
                    openStorage();
                } catch (IOException e2) {
                    throw new LedgerException(e);
                }
            }
            opened = true;
        } catch (IOException e) {
            // NoSuchFileException for incorrect file path
            cleanup(e);
            throw new LedgerException(e);
        } catch (LedgerException e) {
            cleanup(e);
            throw e;
        }
    }

    private void openStorage() {
        // TODO:
    }

    private void rebuildIndex() {
        throw new UnsupportedOperationException();// TODO:
    }

    private static void writePid(@Nonnull FileChannel lockChannel) throws IOException {

        lockChannel.write(ByteBuffer.wrap(String.valueOf(ProcessHandle.current().pid()).getBytes(UTF_8)), 0);
        lockChannel.force(true);
    }

    private static boolean isProcessAlive(long pid) {
        return ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false);
    }

    private static long readPid(@Nonnull FileChannel lockChannel) throws IOException, LedgerException {

        final var size = lockChannel.size();
        if (size == 0) {
            throw new LedgerException();
        }
        if (size > MAX_PID_LENGTH) {
            throw new LedgerException();
        }
        final var byteBuffer = ByteBuffer.allocate((int) size);
        lockChannel.read(byteBuffer, 0);
        byteBuffer.flip();
        final var bytes = new byte[byteBuffer.limit()];
        byteBuffer.get(bytes);
        try {
            return Long.parseLong(new String(bytes, UTF_8));
        } catch (NumberFormatException e) {
            throw new LedgerException();
        }
    }

    @Override
    public void close() throws LedgerException {

        cleanup(null);

        if (opened) {
            opened = false;
            try {
                Files.delete(lockPath);
            } catch (IOException e) {
                throw new LedgerException(e);
            }
        }
    }

    private void cleanup(@Nullable Throwable suppressed) throws LedgerException {

        try {
            if (fileLock != null) {
                fileLock.close();
            }

            if (lockChannel != null) {
                lockChannel.close();
            }
        } catch (Exception e) {
            final var ledgerException = new LedgerException(e);
            if (suppressed != null) {
                ledgerException.addSuppressed(suppressed);
            }
            throw ledgerException;
        }
    }

    @Nonnull
    public Slot slot() {
        return null;// TODO:
    }

    @Nonnull
    public Slot slot(long sequence, long offset) {
        return null;// TODO:
    }

    @Nonnull
    public Watcher watch() {
        return null;// TODO:
    }

    @Nonnull
    public Watcher watch(long sequence, long offset) {
        return null;// TODO:
    }
}
