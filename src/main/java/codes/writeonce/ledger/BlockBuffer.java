package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

public class BlockBuffer {

    @Nonnull
    private final ByteBuffer writerByteBuffer;

    @Nonnull
    private final ByteBuffer readerByteBuffer;

    public BlockBuffer(@Nonnull ByteBuffer writerByteBuffer) {
        this.writerByteBuffer = writerByteBuffer;
        this.readerByteBuffer = writerByteBuffer.duplicate();
    }

    @Nonnull
    public ByteBuffer getWriterByteBuffer() {
        return writerByteBuffer;
    }

    @Nonnull
    public ByteBuffer getReaderByteBuffer() {
        return readerByteBuffer;
    }
}
