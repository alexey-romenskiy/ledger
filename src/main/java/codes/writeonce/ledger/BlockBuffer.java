package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockBuffer {

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

    public final AtomicInteger refCount = new AtomicInteger(1);

    public BlockBuffer(@Nonnull ByteBuffer writeBuffer, @Nonnull ByteBuffer diskBuffer,
            @Nonnull ByteBuffer subscriberBuffer) {
        this.writeBuffer = writeBuffer;
        this.diskBuffer = diskBuffer;
        this.subscriberBuffer = subscriberBuffer;
    }
}
