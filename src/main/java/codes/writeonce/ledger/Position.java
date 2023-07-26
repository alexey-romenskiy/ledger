package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.util.Objects;

public class Position implements Comparable<Position> {

    /**
     * Last fully completed message's sequence.
     */
    public long sequence;

    /**
     * Last incomplete message's offset.
     */
    public long offset;

    public Position(long sequence, long offset) {

        if (sequence <= 0) {
            throw new IllegalArgumentException();
        }

        if (offset < 0) {
            throw new IllegalArgumentException();
        }

        this.sequence = sequence;
        this.offset = offset;
    }

    @Override
    public int compareTo(Position o) {

        if (sequence < o.sequence) {
            return -1;
        }
        if (sequence > o.sequence) {
            return 1;
        }
        return Long.compare(offset, o.offset);
    }

    public int compareTo(long sequence, long offset) {

        if (this.sequence < sequence) {
            return -1;
        }
        if (this.sequence > sequence) {
            return 1;
        }
        return Long.compare(this.offset, offset);
    }

    public boolean equals(long sequence, long offset) {

        return this.sequence == sequence && this.offset == offset;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final var that = (Position) o;
        return sequence == that.sequence && offset == that.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sequence, offset);
    }

    @Nonnull
    public Position copy() {
        return new Position(sequence, offset);
    }

    public void set(long sequence, long offset) {
        this.sequence = sequence;
        this.offset = offset;
    }

    public void set(@Nonnull Position position) {
        this.sequence = position.sequence;
        this.offset = position.offset;
    }

    @Override
    public String toString() {
        return "{sequence=" + sequence +
               ", offset=" + offset +
               '}';
    }
}
