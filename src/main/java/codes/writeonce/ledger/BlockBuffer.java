package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

public class BlockBuffer {

    @Nonnull
    private final ByteBuffer byteBuffer;

    @Nonnull
    private final ByteBuffer duplicateByteBuffer;

    public BlockBuffer(@Nonnull ByteBuffer writerByteBuffer) {
        this.byteBuffer = writerByteBuffer;
        this.duplicateByteBuffer = writerByteBuffer.duplicate();
    }

    @Nonnull
    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    @Nonnull
    public ByteBuffer getDuplicateByteBuffer() {
        return duplicateByteBuffer;
    }
}
