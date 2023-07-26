package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

public abstract class AbstractMessageReceiver implements MessageReceiver {

    private final long neededSequence;
    private final long neededOffset;
    private long lastSequence;
    private long lastOffset;
    private boolean first = true;

    public AbstractMessageReceiver(long neededSequence, long neededOffset) {
        this.neededSequence = neededSequence;
        this.neededOffset = neededOffset;
    }

    @Override
    public void next(long sequence, long offset, boolean last, @Nonnull ByteBuffer byteBuffer) {

        if (sequence <= 0) {
            throw new IllegalArgumentException();
        }

        if (offset < 0) {
            throw new IllegalArgumentException();
        }

        if (first) {
            if (sequence == neededSequence) {
                if (offset > neededOffset) {
                    throw new IllegalArgumentException();
                }
            } else if (sequence > neededSequence) {
                if (offset != 0) {
                    throw new IllegalArgumentException();
                }
            }
            first = false;
        } else {
            if (sequence == lastSequence) {
                if (offset != lastOffset) {
                    throw new IllegalArgumentException();
                }
            } else if (sequence > lastSequence) {
                if (offset != 0) {
                    throw new IllegalArgumentException();
                }
                reset(sequence - lastSequence);
            } else {
                throw new IllegalArgumentException();
            }
        }

        final var available = byteBuffer.remaining();

        if (last) {
            lastSequence = sequence + 1;
            lastOffset = 0;
        } else {
            lastSequence = sequence;
            lastOffset = offset + available;
        }

        if (sequence > neededSequence) {
            process(sequence, offset, last, byteBuffer);
        } else if (sequence == neededSequence) {
            if (offset >= neededOffset) {
                process(sequence, offset, last, byteBuffer);
            } else if (offset + available >= neededOffset) {
                // partially skip
                final var start = (int) (neededOffset - offset);
                byteBuffer.position(byteBuffer.position() + start);
                process(sequence, offset + start, last, byteBuffer);
            }
        }
    }

    protected abstract void reset(long n);

    protected abstract void process(long sequence, long offset, boolean last, @Nonnull ByteBuffer byteBuffer);
}
