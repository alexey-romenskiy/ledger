package codes.writeonce.ledger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class Ledger implements AutoCloseable {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final long topicId;

    @Nonnull
    private final AtomicBoolean closed = new AtomicBoolean();

    @Nonnull
    private final MessageWriter messageWriter;

    @Nonnull
    private final BlockWriterImpl blockWriter;

    @Nonnull
    private final Path lockPath;

    @Nonnull
    private final FileChannel lockChannel;

    @Nonnull
    private final FileLock fileLock;

    public Ledger(
            long topicId,
            @Nonnull MessageWriter messageWriter,
            @Nonnull BlockWriterImpl blockWriter,
            @Nonnull Path lockPath,
            @Nonnull FileChannel lockChannel,
            @Nonnull FileLock fileLock
    ) {
        this.topicId = topicId;
        this.messageWriter = messageWriter;
        this.blockWriter = blockWriter;
        this.lockPath = lockPath;
        this.lockChannel = lockChannel;
        this.fileLock = fileLock;
    }

    @Nonnull
    public MessageWriter getMessageWriter() {
        return messageWriter;
    }

    @Nonnull
    public BlockWriterImpl getBlockWriter() {
        return blockWriter;
    }

    @Nullable
    public Slot slot(long id) {
        return blockWriter.slot(id);
    }

    public boolean deleteSlot(long id) {
        return blockWriter.deleteSlot(id);
    }

    @Nonnull
    public Slot createSlot() {
        return blockWriter.createSlot();
    }

    @Nullable
    public Slot createSlot(long sequence, long offset) {
        return blockWriter.createSlot(sequence, offset);
    }

    @Override
    public void close() {

        if (blockWriter.close()) {
            if (!closed.getAndSet(true)) {
                try {
                    try {
                        Files.delete(lockPath);
                    } catch (NoSuchFileException ignore) {
                        // ignore
                    }
                    fileLock.close();
                    lockChannel.close();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            logger.info("LEDGER#{}: Closed", topicId);
        } else {
            logger.error("LEDGER#{}: Not closed normally", topicId);
        }
    }

    public long getTopicId() {
        return topicId;
    }

    public long getBlockFilePosition() {
        return blockWriter.getBlockFilePosition();
    }

    public long getBlockFileRemained() {
        return blockWriter.getBlockFileRemained();
    }

    public int getQueuePosition() {
        return blockWriter.getQueuePosition();
    }

    @Nonnull
    public Map<Long, Map<String, Object>> getSlotInfos() {
        return blockWriter.getSlotInfos();
    }

    public int getBlockSize() {
        return blockWriter.getBlockSize();
    }

    public int getQueueSize() {
        return blockWriter.getQueueSize();
    }

    public int getQueueRemained() {
        return blockWriter.getQueueRemained();
    }

    public long getFirstNeededBlockFilePosition() {
        return blockWriter.getFirstNeededBlockFilePosition();
    }

    public long getBlockFileSize() {
        return blockWriter.getBlockFileSize();
    }
}
