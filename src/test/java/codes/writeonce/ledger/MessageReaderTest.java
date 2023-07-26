package codes.writeonce.ledger;

import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

import static codes.writeonce.ledger.TestUtils.arrayToString;
import static java.nio.ByteBuffer.allocate;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class MessageReaderTest {

    private static final long TOPIC_ID = 1;

    private static final int BLOCK_SIZE = 48;

    private static final byte[] SAMPLE = "thequickbrownfoxjumpsoverthelazydog".getBytes(ISO_8859_1);

    private ArrayList<ByteBuffer> byteBuffers;

    private MessageWriter messageWriter;

    @Before
    public void before() {
        byteBuffers = new ArrayList<>();
        final var firstBuffer = allocate(BLOCK_SIZE);
        messageWriter = new MessageWriter(TOPIC_ID, BLOCK_SIZE,
                new BlockWriter() {

                    @Nonnull
                    private ByteBuffer byteBuffer = firstBuffer;

                    @Nonnull
                    @Override
                    public ByteBuffer fullBlock(
                            long messageSequence,
                            long accumulatedMessageLength
                    ) {
                        byteBuffers.add(byteBuffer);
                        byteBuffer = allocate(BLOCK_SIZE);
                        return byteBuffer;
                    }

                    @Override
                    public void partialBlock(
                            long messageSequence,
                            long accumulatedMessageLength,
                            int dataSize
                    ) {
                        // empty
                    }
                },
                firstBuffer);
    }

    @Test
    public void next() {
        final var invocations = new ArrayList<Invocation>();
        final var messageReader = new MessageReader();
        messageWriter.chunk(true, false, 0, 18, SAMPLE);
        final var receiver = new MessageReceiver() {
            @Override
            public void next(long sequence, long offset, boolean last, @Nonnull ByteBuffer byteBuffer) {
                final var bytes = new byte[byteBuffer.remaining()];
                byteBuffer.get(bytes);
                invocations.add(new Invocation(sequence, offset, last, bytes));
            }
        };
        for (final var byteBuffer : byteBuffers) {
            byteBuffer.clear();
            messageReader.next(byteBuffer, receiver);
        }
        assertEquals(singletonList(new Invocation(1, 0, true, "thequickbrownfoxju".getBytes(ISO_8859_1))),
                invocations);
    }

    private static class Invocation {

        public final long sequence;

        public final long offset;

        public final boolean last;

        @Nonnull
        public final byte[] bytes;

        public Invocation(long sequence, long offset, boolean last, @Nonnull byte[] bytes) {
            this.sequence = sequence;
            this.offset = offset;
            this.last = last;
            this.bytes = bytes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Invocation that = (Invocation) o;
            return sequence == that.sequence && offset == that.offset && last == that.last &&
                   Arrays.equals(bytes, that.bytes);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(sequence, offset, last);
            result = 31 * result + Arrays.hashCode(bytes);
            return result;
        }

        @Override
        public String toString() {
            return "Invocation{" +
                   "sequence=" + sequence +
                   ", offset=" + offset +
                   ", last=" + last +
                   ", bytes=" + arrayToString(bytes) +
                   '}';
        }
    }
}
