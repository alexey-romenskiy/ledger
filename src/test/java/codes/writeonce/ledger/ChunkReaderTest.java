package codes.writeonce.ledger;

import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import static java.nio.ByteBuffer.allocate;
import static java.nio.charset.StandardCharsets.ISO_8859_1;

public class ChunkReaderTest {

    private static final int BLOCK_SIZE = 48;

    private static final byte[] SAMPLE = "thequickbrownfoxjumpsoverthelazydog".getBytes(ISO_8859_1);

    private ArrayList<ByteBuffer> byteBuffers;

    private ChunkWriter chunkWriter;

    @Before
    public void before() {
        byteBuffers = new ArrayList<>();
        chunkWriter = new ChunkWriter(BLOCK_SIZE, new Pool<>() {
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
                byteBuffers.add(blockBuffer.getWriterByteBuffer());
            }

            @Override
            public void partialBlock(
                    long sequence,
                    long offset,
                    boolean pending,
                    @Nonnull BlockBuffer blockBuffer,
                    int end
            ) {
                // empty
            }
        });
    }

    @Test
    public void next() throws InterruptedException {
        final var chunkReader = new ChunkReader(BLOCK_SIZE, new Receiver() {
            @Override
            public void next(long sequence, long offset, boolean last, @Nonnull ByteBuffer byteBuffer) {
                System.out.println("sequence = " + sequence);
                System.out.println("offset = " + offset);
                System.out.println("last = " + last);
                final var bytes = new byte[byteBuffer.remaining()];
                byteBuffer.get(bytes);
                System.out.println("buffer = '" + new String(bytes, ISO_8859_1) + "'");
            }
        });
        chunkWriter.sequence(1);
        chunkWriter.chunk(true, false, 0, 18, SAMPLE);
        for (final var byteBuffer : byteBuffers) {
            chunkReader.next(byteBuffer);
        }
    }
}
