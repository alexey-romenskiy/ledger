package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class BufferPool implements Pool<BlockBuffer> {

    private final BlockBuffer[] pool;

    private final AtomicInteger index = new AtomicInteger();

    private final byte[] zeroes;

    public BufferPool(int blockSize, int buffersPoolSize) {

        zeroes = new byte[blockSize];

        pool = new BlockBuffer[buffersPoolSize];

        final var wholeBuffer = ByteBuffer.allocateDirect(blockSize * (buffersPoolSize + 1) - 1);
        final var addressModulus = wholeBuffer.alignmentOffset(0, blockSize);
        final var alignedPosition = addressModulus > 0 ? blockSize - addressModulus : 0;

        for (int i = 0; i < buffersPoolSize; i++) {
            final var position = alignedPosition + i * blockSize;
            pool[i] = new BlockBuffer(
                    wholeBuffer.slice(position, blockSize),
                    wholeBuffer.slice(position, blockSize),
                    wholeBuffer.slice(position, blockSize)
            );
        }
    }

    @Nonnull
    public BlockBuffer acquire() {

        while (true) {
            final var i = index.getAndSet(-1);
            if (i != -1) {
                if (i < pool.length) {
                    final var value = pool[i];
                    pool[i] = null;
                    index.set(i + 1);
                    return value;
                } else {
                    index.set(i);
                    throw new IllegalStateException();
                }
            }
        }
    }

    public void release(@Nonnull BlockBuffer value) {

        value.writeBuffer.clear();
        value.diskBuffer.clear();
        value.subscriberBuffer.clear();
        value.writeBuffer.put(0, zeroes);

        while (true) {
            final var i = index.getAndSet(-1);
            if (i != -1) {
                if (i > 0) {
                    final var i2 = i - 1;
                    pool[i2] = value;
                    index.set(i2);
                    return;
                } else {
                    index.set(i);
                    throw new IllegalStateException();
                }
            }
        }
    }
}
