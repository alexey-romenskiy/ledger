package codes.writeonce.ledger;

import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Objects;
import java.util.stream.IntStream;

import static java.nio.ByteBuffer.allocate;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;

public class ChunkWriterTest {

    private static final int BLOCK_SIZE = 48;

    private static final byte[] SAMPLE = "thequickbrownfoxjumpsoverthelazydog".getBytes(ISO_8859_1);

    private ArrayList<Invocation> invocations;

    private ChunkWriter chunkWriter;

    @Before
    public void before() {
        invocations = new ArrayList<>();
        chunkWriter = getChunkWriter(invocations);
    }

    @Test
    public void chunk1() throws InterruptedException {
        chunkWriter.sequence(1);
        chunkWriter.chunk(true, false, 0, 26, SAMPLE);
        assertEquals(singletonList(new FullInvocation(1, 26, false,
                allocate(BLOCK_SIZE).putLong(-1).putLong(0).putShort((short) 26)
                        .put("thequickbrownfoxjumpsovert".getBytes(ISO_8859_1))
                        .putInt(0x5920138c), 0)), invocations);
    }

    @Test
    public void chunk2() throws InterruptedException {
        chunkWriter.sequence(1);
        chunkWriter.chunk(true, true, 0, 3, SAMPLE);
        assertEquals(singletonList(new PartialInvocation(1, 3, false,
                allocate(BLOCK_SIZE).putLong(-1).putLong(0).putShort((short) 3)
                        .put("the".getBytes(ISO_8859_1))
                        .putInt(0x185eeaa7), 25)), invocations);
    }

    @Test
    public void chunk2b() throws InterruptedException {
        chunkWriter.sequence(1);
        chunkWriter.chunk(true, true, 0, 4, SAMPLE);
        assertEquals(singletonList(new PartialInvocation(1, 4, false,
                allocate(BLOCK_SIZE).putLong(-1).putLong(0).putShort((short) 4)
                        .put("theq".getBytes(ISO_8859_1))
                        .putInt(0x0f8a1a96), 26)), invocations);
    }

    @Test
    public void chunk2c() throws InterruptedException {
        chunkWriter.sequence(1);
        chunkWriter.chunk(true, true, 0, 4, SAMPLE);
        chunkWriter.sequence(2);
        chunkWriter.last();
        assertEquals(asList(
                new PartialInvocation(
                        1,
                        4,
                        false,
                        allocate(BLOCK_SIZE)
                                .putLong(-1)
                                .putLong(0)
                                .putShort((short) 4)
                                .put("theq".getBytes(ISO_8859_1))
                                .putInt(0x0f8a1a96),
                        26
                ),
                new FullInvocation(
                        2,
                        0,
                        false,
                        allocate(BLOCK_SIZE)
                                .putLong(-1)
                                .putLong(0)
                                .putShort((short) 4)
                                .put("theq".getBytes(ISO_8859_1))
                                .putInt(0x0f8a1a96)
                                .putLong(-2)
                                .putLong(0)
                                .putShort((short) 0)
                                .putInt(0x1035a607),
                        0
                )
        ), invocations);
    }

    @Test
    public void chunk3() throws InterruptedException {
        chunkWriter.sequence(1);
        chunkWriter.chunk(false, false, 0, 3, SAMPLE);
        chunkWriter.chunk(true, false, 3, 9, SAMPLE);
        assertEquals(singletonList(new FullInvocation(1, 9, false,
                allocate(BLOCK_SIZE).putLong(-1).putLong(0).putShort((short) 9)
                        .put("thequickb".getBytes(ISO_8859_1))
                        .putInt(0x213c8b28), 17)), invocations);
    }

    @Nonnull
    private ChunkWriter getChunkWriter(@Nonnull ArrayList<Invocation> invocations) {

        return new ChunkWriter(BLOCK_SIZE, new Pool<>() {
            @Nonnull
            @Override
            public BlockBuffer borrow() {
                return new BlockBuffer(allocate(BLOCK_SIZE));
            }

            @Override
            public void reclaim(@Nonnull BlockBuffer value) {
                // empty
            }
        }, new BlockWriter() {
            @Override
            public void fullBlock(long sequence, long offset, boolean pending, @Nonnull BlockBuffer blockBuffer,
                    int remaining) {
                invocations.add(new FullInvocation(sequence, offset, pending, blockBuffer.getWriterByteBuffer(),
                        remaining));
            }

            @Override
            public void partialBlock(
                    long sequence,
                    long offset,
                    boolean pending,
                    @Nonnull BlockBuffer blockBuffer,
                    int end
            ) {
                invocations.add(new PartialInvocation(
                        sequence,
                        offset,
                        pending,
                        allocate(blockBuffer.getWriterByteBuffer().capacity())
                                .put(blockBuffer.getWriterByteBuffer().duplicate().clear()),
                        end
                ));
            }
        });
    }

    private interface Invocation {
        // empty
    }

    private static class FullInvocation implements Invocation {

        private final long sequence;

        private final long offset;

        private final boolean pending;

        @Nonnull
        private final ByteBuffer byteBuffer;

        private final int remaining;

        public FullInvocation(long sequence, long offset, boolean pending, @Nonnull ByteBuffer byteBuffer,
                int remaining) {
            this.sequence = sequence;
            this.offset = offset;
            this.pending = pending;
            byteBuffer.position(0);
            byteBuffer.limit(byteBuffer.capacity() - remaining);
            this.byteBuffer = byteBuffer;
            this.remaining = remaining;
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
            return sequence == that.sequence && offset == that.offset && pending == that.pending &&
                   remaining == that.remaining && byteBuffer.equals(that.byteBuffer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sequence, offset, pending, byteBuffer, remaining);
        }

        @Override
        public String toString() {
            return "FullInvocation{" +
                   "sequence=" + sequence +
                   ", offset=" + offset +
                   ", pending=" + pending +
                   ", byteBuffer=" + arrayToString(byteBuffer.array()) +
                   ", remaining=" + remaining +
                   '}';
        }
    }

    private static class PartialInvocation implements Invocation {

        private final long sequence;

        private final long offset;

        private final boolean pending;

        @Nonnull
        private final ByteBuffer byteBuffer;

        private final int end;

        public PartialInvocation(long sequence, long offset, boolean pending, @Nonnull ByteBuffer byteBuffer, int end) {
            this.sequence = sequence;
            this.offset = offset;
            this.pending = pending;
            byteBuffer.position(0);
            byteBuffer.limit(end);
            this.byteBuffer = byteBuffer;
            this.end = end;
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
            return sequence == that.sequence && offset == that.offset && pending == that.pending && end == that.end &&
                   byteBuffer.equals(that.byteBuffer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sequence, offset, pending, byteBuffer, end);
        }

        @Override
        public String toString() {
            return "PartialInvocation{" +
                   "sequence=" + sequence +
                   ", offset=" + offset +
                   ", pending=" + pending +
                   ", byteBuffer=" + arrayToString(byteBuffer.array()) +
                   ", end=" + end +
                   '}';
        }
    }

    private static String arrayToString(byte[] array) {
        return IntStream.range(0, array.length)
                .map(i -> array[i] & 0xff)
                .mapToObj(n -> n < BLOCK_SIZE ? String.format("%02X", n) : String.format("%02X|%c", n, (char) n))
                .collect(joining(", "));
    }
}
