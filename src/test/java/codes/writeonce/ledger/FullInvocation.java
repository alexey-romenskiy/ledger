package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.Objects;

import static codes.writeonce.ledger.TestUtils.arrayToString;

class FullInvocation implements Invocation {

    private final long sequence;

    private final long offset;

    private final boolean finished;

    @Nonnull
    private final ByteBuffer byteBuffer;

    public FullInvocation(
            long sequence,
            long offset,
            boolean finished,
            @Nonnull ByteBuffer byteBuffer
    ) {
        this.sequence = sequence;
        this.offset = offset;
        this.finished = finished;
        byteBuffer.position(0);
        byteBuffer.limit(byteBuffer.capacity());
        this.byteBuffer = byteBuffer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FullInvocation that = (FullInvocation) o;
        return sequence == that.sequence && offset == that.offset && finished == that.finished &&
               byteBuffer.equals(that.byteBuffer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sequence, offset, finished, byteBuffer);
    }

    @Override
    public String toString() {
        return "FullInvocation{" +
               "sequence=" + sequence +
               ", offset=" + offset +
               ", finished=" + finished +
               ", byteBuffer=" + arrayToString(byteBuffer.array()) +
               '}';
    }
}
