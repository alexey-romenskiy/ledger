package codes.writeonce.ledger;

import codes.writeonce.concurrency.Bloom;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

/**
 * blockSize >= availableDataLength >= writingSize >= stableSize >= 0
 */
public class BlockBuffer {

    public int index = -1;

    /**
     * For writing to buffer.
     */
    @Nonnull
    public final ByteBuffer writeBuffer;

    /**
     * For writing to disk.
     */
    @Nonnull
    public final ByteBuffer diskBuffer;

    /**
     * For cloning by subscribers.
     */
    @Nonnull
    public final ByteBuffer subscriberBuffer;

    public int refCount = 1;

    public boolean cached;

    public final Bloom dataBloom = new Bloom();

    public volatile int availableDataLength;

    public volatile boolean readFailed;

    public long blockFilePosition = -1;

    public boolean writing;

    public int writingSize;

    public long writingStartNanos;

    public volatile int stableSize;

    public volatile Throwable writeFailed;

    public final Bloom stabBloom = new Bloom();

    public long readySequence;

    public long readyOffset;

    public long writingSequence;

    public long writingOffset;

    public long stableSequence;

    public long stableOffset;

    public BlockBuffer(
            @Nonnull ByteBuffer writeBuffer,
            @Nonnull ByteBuffer diskBuffer,
            @Nonnull ByteBuffer subscriberBuffer
    ) {
        this.writeBuffer = writeBuffer;
        this.diskBuffer = diskBuffer;
        this.subscriberBuffer = subscriberBuffer;
    }
}
