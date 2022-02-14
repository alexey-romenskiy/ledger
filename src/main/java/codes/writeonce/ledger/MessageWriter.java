package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import static java.util.Objects.requireNonNull;

/**
 * 8 bytes signed   | sequence | nonzero, negative for the last message's chunk
 * 8 bytes signed   | chunk data byte offset within the message | must be non-negative
 * 2 bytes unsigned | chunk data bytes size
 * ...              | chunk data bytes
 * 4 bytes          | checksum
 */
public class MessageWriter {

    public static final int HEADER_SIZE = 18;

    public static final int TRAILER_SIZE = 4;

    public static final int MAX_CHUNK_DATA_SIZE = 0xffff;

    private final CRC32 checksum = new CRC32();

    private final int blockSize;

    @Nonnull
    private final BlockWriter blockWriter;

    private final int maxChunkDataSize;

    @Nonnull
    private ByteBuffer byteBuffer;

    private boolean messageFinished;

    private long messageSequence;

    private long messageLength;

    private int chunkEndPosition;

    private int maxChunkEndPosition;

    private int chunkHeaderPosition;

    private boolean hasLastChunkNotFlushed;

    public MessageWriter(int blockSize, @Nonnull BlockWriter blockWriter, @Nonnull ByteBuffer byteBuffer) {
        this(blockSize, blockWriter, byteBuffer, 0, MAX_CHUNK_DATA_SIZE, 0, 0, true);
    }

    public MessageWriter(
            int blockSize,
            @Nonnull BlockWriter blockWriter,
            @Nonnull ByteBuffer byteBuffer,
            int position,
            int maxChunkDataSize,
            long sequence,
            long offset,
            boolean last
    ) {
        if (maxChunkDataSize < 1) {
            throw new IllegalArgumentException();
        }

        if (maxChunkDataSize > MAX_CHUNK_DATA_SIZE) {
            throw new IllegalArgumentException();
        }

        if (blockSize < HEADER_SIZE + TRAILER_SIZE + 1) {
            throw new IllegalArgumentException();
        }

        if (byteBuffer.capacity() != blockSize) {
            throw new IllegalArgumentException();
        }

        if (position < 0) {
            throw new IllegalArgumentException();
        }

        if (position > blockSize - (HEADER_SIZE + TRAILER_SIZE)) {
            throw new IllegalArgumentException();
        }

        if (sequence < 0) {
            throw new IllegalArgumentException();
        }

        if (sequence == 0) {
            if (offset != 0) {
                throw new IllegalArgumentException();
            }
            if (!last) {
                throw new IllegalArgumentException();
            }
            messageFinished = true;
        } else {
            if (offset < 0) {
                throw new IllegalArgumentException();
            }
            messageSequence = sequence;
            messageLength = offset;
            messageFinished = last;
        }

        this.blockSize = blockSize;
        this.blockWriter = requireNonNull(blockWriter);
        this.maxChunkDataSize = maxChunkDataSize;
        this.byteBuffer = byteBuffer;
        this.chunkHeaderPosition = position;
        adjust();
    }

    public void sequence(long nextSequence) {

        if (!messageFinished) {
            throw new IllegalStateException();
        }

        if (messageSequence >= nextSequence) {
            throw new IllegalStateException();
        }

        if (messageSequence != 0 && nextSequence != messageSequence + 1) {
            throw new IllegalStateException();
        }

        messageSequence = nextSequence;
        messageLength = 0;
        messageFinished = false;
    }

    public boolean isMessageFinished() {
        return messageFinished;
    }

    public long getMessageSequence() {
        return messageSequence;
    }

    public long getMessageLength() {
        return messageLength;
    }

    @Nonnull
    public ByteBuffer chunk() {

        if (messageFinished) {
            throw new IllegalStateException();
        }

        updatePosition();

        if (chunkEndPosition < maxChunkEndPosition) {
            return byteBuffer;
        }

        byteBuffer.putLong(chunkHeaderPosition, messageSequence);
        fillChunk();

        final var bufferRemaining = blockSize - chunkEndPosition - TRAILER_SIZE;
        if (bufferRemaining > HEADER_SIZE + TRAILER_SIZE) {
            nextChunk();
        } else {
            sendFinal(bufferRemaining);
        }

        return byteBuffer;
    }

    public void chunk(boolean last, boolean endOfBatch, int start, int end, @Nonnull byte[] bytes) {

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

    public void last() {

        if (messageFinished) {
            throw new IllegalStateException();
        }

        updatePosition();

        byteBuffer.putLong(chunkHeaderPosition, -messageSequence);
        fillChunk();
        hasLastChunkNotFlushed = true;
        messageFinished = true;

        final var bufferRemaining = blockSize - chunkEndPosition - TRAILER_SIZE;
        if (bufferRemaining < HEADER_SIZE + TRAILER_SIZE) {
            sendFinal(bufferRemaining);
        } else {
            nextChunk();
        }
    }

    public void endOfBatch() {

        if (hasLastChunkNotFlushed) {
            blockWriter.partialBlock(messageSequence, messageLength, messageFinished, chunkHeaderPosition);
            hasLastChunkNotFlushed = false;
        }
    }

    private void updatePosition() {

        final var bufferPosition = byteBuffer.position();

        if (bufferPosition < chunkEndPosition) {
            throw new IllegalArgumentException();
        }

        final var limit = byteBuffer.limit();

        if (limit != maxChunkEndPosition) {
            throw new IllegalArgumentException();
        }

        if (bufferPosition > limit) {
            throw new IllegalArgumentException();
        }

        chunkEndPosition = bufferPosition;
    }

    private void clearChunk() {

        hasLastChunkNotFlushed = false;
        chunkHeaderPosition = 0;
        adjust();
    }

    private void nextChunk() {

        chunkHeaderPosition = chunkEndPosition + TRAILER_SIZE;
        adjust();
    }

    private void adjust() {

        chunkEndPosition = chunkHeaderPosition + HEADER_SIZE;
        maxChunkEndPosition = Math.min(chunkEndPosition + maxChunkDataSize, blockSize - TRAILER_SIZE);
        byteBuffer.limit(maxChunkEndPosition);
        byteBuffer.position(chunkEndPosition);
    }

    private void fillChunk() {

        byteBuffer.putLong(chunkHeaderPosition + 8, messageLength);
        final var length = chunkEndPosition - chunkHeaderPosition - HEADER_SIZE;
        messageLength += length;
        byteBuffer.putShort(chunkHeaderPosition + 16, (short) length);
        byteBuffer.position(chunkHeaderPosition);
        byteBuffer.limit(chunkEndPosition);
        checksum.reset();
        checksum.update(byteBuffer);
        byteBuffer.limit(blockSize);
        byteBuffer.putInt((int) checksum.getValue());
    }

    private void sendFinal(int remaining) {

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

        byteBuffer = blockWriter.fullBlock(messageSequence, messageLength, messageFinished);
        clearChunk();
    }
}
