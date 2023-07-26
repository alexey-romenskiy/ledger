package codes.writeonce.ledger;

import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import static codes.writeonce.ledger.TestUtils.BLOCK_SIZE;
import static java.nio.ByteBuffer.allocate;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class MessageWriterTest {

    private static final long TOPIC_ID = 1;

    private static final byte[] SAMPLE = "thequickbrownfoxjumpsoverthelazydog".getBytes(ISO_8859_1);

    private ArrayList<Invocation> invocations;

    private MessageWriter messageWriter;

    @Before
    public void before() {
        invocations = new ArrayList<>();
        messageWriter = getMessageWriter(invocations);
    }

    @Test
    public void chunk1() {
        messageWriter.chunk(true, false, 0, 26, SAMPLE);
        assertEquals(singletonList(new FullInvocation(2, 0,
                allocate(BLOCK_SIZE).putLong(-1).putLong(0).putShort((short) 26)
                        .put("thequickbrownfoxjumpsovert".getBytes(ISO_8859_1))
                        .putInt(0x5920138c))), invocations);
    }

    @Test
    public void chunk2() {
        messageWriter.chunk(true, true, 0, 3, SAMPLE);
        assertEquals(singletonList(new PartialInvocation(2, 0,
                allocate(BLOCK_SIZE).putLong(-1).putLong(0).putShort((short) 3)
                        .put("the".getBytes(ISO_8859_1))
                        .putInt(0x185eeaa7), 25)), invocations);
    }

    @Test
    public void chunk2b() {
        messageWriter.chunk(true, true, 0, 4, SAMPLE);
        assertEquals(singletonList(new PartialInvocation(2, 0,
                allocate(BLOCK_SIZE).putLong(-1).putLong(0).putShort((short) 4)
                        .put("theq".getBytes(ISO_8859_1))
                        .putInt(0x0f8a1a96), 26)), invocations);
    }

    @Test
    public void chunk2c() {
        messageWriter.chunk(true, true, 0, 4, SAMPLE);
        messageWriter.last();
        assertEquals(asList(
                new PartialInvocation(
                        2,
                        0,
                        allocate(BLOCK_SIZE)
                                .putLong(-1)
                                .putLong(0)
                                .putShort((short) 4)
                                .put("theq".getBytes(ISO_8859_1))
                                .putInt(0x0f8a1a96),
                        26
                ),
                new FullInvocation(
                        3,
                        0,
                        allocate(BLOCK_SIZE)
                                .putLong(-1)
                                .putLong(0)
                                .putShort((short) 4)
                                .put("theq".getBytes(ISO_8859_1))
                                .putInt(0x0f8a1a96)
                                .putLong(-2)
                                .putLong(0)
                                .putShort((short) 0)
                                .putInt(0x1035a607)
                )
        ), invocations);
    }

    @Test
    public void chunk3() {
        messageWriter.chunk(false, false, 0, 3, SAMPLE);
        messageWriter.chunk(true, false, 3, 9, SAMPLE);
        assertEquals(singletonList(new FullInvocation(2, 0,
                allocate(BLOCK_SIZE).putLong(-1).putLong(0).putShort((short) 9)
                        .put("thequickb".getBytes(ISO_8859_1))
                        .putInt(0x213c8b28))), invocations);
    }

    @Nonnull
    private MessageWriter getMessageWriter(@Nonnull ArrayList<Invocation> invocations) {

        final var firstBuffer = allocate(BLOCK_SIZE);

        return new MessageWriter(
                TOPIC_ID, BLOCK_SIZE,
                new BlockWriter() {

                    @Nonnull
                    private ByteBuffer byteBuffer = firstBuffer;

                    @Nonnull
                    @Override
                    public ByteBuffer fullBlock(
                            long messageSequence,
                            long accumulatedMessageLength
                    ) {
                        assertEquals(0, byteBuffer.remaining());
                        assertEquals(BLOCK_SIZE, byteBuffer.limit());
                        invocations.add(new FullInvocation(
                                messageSequence,
                                accumulatedMessageLength,
                                byteBuffer
                        ));
                        byteBuffer = allocate(BLOCK_SIZE);
                        return byteBuffer;
                    }

                    @Override
                    public void partialBlock(
                            long messageSequence,
                            long accumulatedMessageLength,
                            int dataSize
                    ) {
                        invocations.add(new PartialInvocation(
                                messageSequence,
                                accumulatedMessageLength,
                                allocate(byteBuffer.capacity()).put(byteBuffer.duplicate().clear()),
                                dataSize
                        ));
                    }
                },
                firstBuffer
        );
    }
}
