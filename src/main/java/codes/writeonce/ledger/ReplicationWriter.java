package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

public class ReplicationWriter implements ReplicationListener {

    @Nonnull
    private final MessageWriter writer;

    @Nonnull
    private final BlockWriterImpl blockWriter;

    public ReplicationWriter(@Nonnull Ledger ledger) {
        this.writer = ledger.getMessageWriter();
        blockWriter = ledger.getBlockWriter();
    }

    @Override
    public void reset(long n) {
        writer.abort(n);
    }

    @Override
    public void process(long sequence, long offset, boolean last, @Nonnull ByteBuffer byteBuffer) {

        var sourceRemaining = byteBuffer.remaining();

        if (sourceRemaining > 0) {
            while (true) {
                final var buffer = writer.chunk();
                final var destinationRemaining = buffer.remaining();
                if (sourceRemaining <= destinationRemaining) {
                    buffer.put(byteBuffer);
                    break;
                }
                final var limit = byteBuffer.limit();
                byteBuffer.limit(byteBuffer.position() + destinationRemaining);
                buffer.put(byteBuffer);
                byteBuffer.limit(limit);
                sourceRemaining -= destinationRemaining;
            }
        }

        if (last) {
            writer.last();
        }
    }

    @Override
    public void readStablePosition(@Nonnull Position position) {
        blockWriter.readStablePosition(position);
    }
}
