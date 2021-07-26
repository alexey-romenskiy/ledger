package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

public class QueueItem {

    public int index;

    @Nonnull
    public final ByteBuffer byteBuffer;

    @Nonnull
    public final ByteBuffer duplicateByteBuffer;

    public final AtomicReference<QueueItem> running = new AtomicReference<>();

    public long fileBlockOffset;

    public int length;

    public QueueItem(int index, @Nonnull ByteBuffer byteBuffer) {
        this.index = index;
        this.byteBuffer = byteBuffer;
        this.duplicateByteBuffer = byteBuffer.duplicate();
    }
}
