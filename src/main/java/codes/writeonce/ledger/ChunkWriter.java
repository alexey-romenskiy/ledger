package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import static java.util.Objects.requireNonNull;

public class ChunkWriter {

    private static final int MAX_CHUNK_DATA_SIZE = 0xffff;

    private final CRC32 checksum = new CRC32();

    private final int blockSize;

    @Nonnull
    private final BlockWriter blockWriter;

    @Nonnull
    private ByteBuffer byteBuffer;

    private final int maxChunkDataSize;

    private boolean pending;

    private long sequence;

    private long offset;

    private int chunkHeaderPosition;

    private int chunkDataEnd;

    private int chunkDataMax;

    private boolean hasLast;

    public ChunkWriter(int blockSize, @Nonnull BlockWriter blockWriter, @Nonnull ByteBuffer byteBuffer) {
        this(blockSize, blockWriter, byteBuffer, MAX_CHUNK_DATA_SIZE);
    }

    public ChunkWriter(int blockSize, @Nonnull BlockWriter blockWriter, @Nonnull ByteBuffer byteBuffer,
            int maxChunkDataSize) {

        if (maxChunkDataSize < 1) {
            throw new IllegalArgumentException();
        }

        if (maxChunkDataSize > MAX_CHUNK_DATA_SIZE) {
            throw new IllegalArgumentException();
        }

        if (blockSize < 23) {
            throw new IllegalArgumentException();
        }

        if (byteBuffer.capacity() != blockSize) {
            throw new IllegalArgumentException();
        }

        this.blockSize = blockSize;
        this.blockWriter = requireNonNull(blockWriter);
        this.byteBuffer = byteBuffer;
        this.maxChunkDataSize = maxChunkDataSize;
        clearChunk();
    }

    public void chunk(boolean last, boolean endOfBatch, int start, int end, @Nonnull byte[] bytes)
            throws InterruptedException {

        requireNonNull(bytes);

        if (start < 0) {
            throw new IllegalArgumentException();
        }

        if (end > bytes.length) {
            throw new IllegalArgumentException();
        }

        if (start > end) {
            throw new IllegalArgumentException();
        }

        while (start < end) {
            final var byteBuffer = chunk();
            final var length = Math.min(end - start, byteBuffer.remaining());
            byteBuffer.put(bytes, start, length);
            start += length;
        }

        if (last) {
            last();
        }

        if (endOfBatch) {
            endOfBatch();
        }
    }

    public void sequence(long sequence) {

        if (pending) {
            throw new IllegalStateException();
        }

        if (this.sequence >= sequence) {
            throw new IllegalStateException();
        }

        this.sequence = sequence;
        offset = 0;
        pending = true;
    }

    public void last() throws InterruptedException {

        if (!pending) {
            throw new IllegalStateException();
        }

        updatePosition();

        byteBuffer.putLong(chunkHeaderPosition, -sequence);
        fillChunk();
        hasLast = true;
        pending = false;

        final var bufferRemaining = blockSize - chunkDataEnd - 4;
        if (bufferRemaining < 22) {
            sendFinal(bufferRemaining);
        } else {
            nextChunk();
        }
    }

    public void endOfBatch() throws InterruptedException {

        if (hasLast) {
            blockWriter.partialBlock(sequence, offset, pending, chunkHeaderPosition);
            hasLast = false;
        }
    }

    @Nonnull
    public ByteBuffer chunk() throws InterruptedException {

        if (!pending) {
            throw new IllegalStateException();
        }

        updatePosition();

        if (chunkDataEnd < chunkDataMax) {
            return byteBuffer;
        }

        byteBuffer.putLong(chunkHeaderPosition, sequence);
        fillChunk();

        final var bufferRemaining = blockSize - chunkDataEnd - 4;
        if (bufferRemaining > 22) {
            nextChunk();
        } else {
            sendFinal(bufferRemaining);
        }

        return byteBuffer;
    }

    private void updatePosition() {

        final var position = byteBuffer.position();
        if (position < chunkDataEnd) {
            throw new IllegalArgumentException();
        }
        final var limit = byteBuffer.limit();
        if (limit != chunkDataMax) {
            throw new IllegalArgumentException();
        }
        if (position > limit) {
            throw new IllegalArgumentException();
        }
        chunkDataEnd = position;
    }

    private void fillChunk() {

        byteBuffer.putLong(chunkHeaderPosition + 8, offset);
        final var length = chunkDataEnd - chunkHeaderPosition - 18;
        offset += length;
        byteBuffer.putShort(chunkHeaderPosition + 16, (short) length);
        byteBuffer.position(chunkHeaderPosition);
        byteBuffer.limit(chunkDataEnd);
        checksum.reset();
        checksum.update(byteBuffer);
        byteBuffer.limit(blockSize);
        byteBuffer.putInt((int) checksum.getValue());
    }

    private void clearChunk() {

        hasLast = false;
        chunkHeaderPosition = 0;
        adjust();
    }

    private void nextChunk() {

        chunkHeaderPosition = chunkDataEnd + 4;
        adjust();
    }

    private void adjust() {

        chunkDataEnd = chunkHeaderPosition + 18;
        chunkDataMax = chunkDataEnd + Math.min(maxChunkDataSize, blockSize - chunkDataEnd - 4);
        byteBuffer.limit(chunkDataMax);
        byteBuffer.position(chunkDataEnd);
    }

    private void sendFinal(int remaining) throws InterruptedException {

        if ((remaining & 1) != 0) {
            byteBuffer.put((byte) 0);
        }
        if ((remaining & 2) != 0) {
            byteBuffer.putShort((short) 0);
        }
        if ((remaining & 4) != 0) {
            byteBuffer.putInt(0);
        }
        if ((remaining & 8) != 0) {
            byteBuffer.putLong(0);
        }
        if ((remaining & 16) != 0) {
            byteBuffer.putLong(0);
            byteBuffer.putLong(0);
        }

        byteBuffer = blockWriter.fullBlock(sequence, offset, pending);
        clearChunk();
    }
}
