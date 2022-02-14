package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.Objects;

import static codes.writeonce.ledger.TestUtils.arrayToString;

class PartialInvocation implements Invocation {

    private final long sequence;

    private final long offset;

    private final boolean finished;

    @Nonnull
    private final ByteBuffer byteBuffer;

    private final int dataSize;

    public PartialInvocation(
            long sequence,
            long offset,
            boolean finished,
            @Nonnull ByteBuffer byteBuffer,
            int dataSize
    ) {
        this.sequence = sequence;
        this.offset = offset;
        this.finished = finished;
        byteBuffer.position(0);
        byteBuffer.limit(dataSize);
        this.byteBuffer = byteBuffer;
        this.dataSize = dataSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartialInvocation that = (PartialInvocation) o;
        return sequence == that.sequence && offset == that.offset && finished == that.finished &&
               dataSize == that.dataSize &&
               byteBuffer.equals(that.byteBuffer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sequence, offset, finished, byteBuffer, dataSize);
    }

    @Override
    public String toString() {
        return "PartialInvocation{" +
               "sequence=" + sequence +
               ", offset=" + offset +
               ", finished=" + finished +
               ", byteBuffer=" + arrayToString(byteBuffer.array()) +
               ", dataSize=" + dataSize +
               '}';
    }
}
