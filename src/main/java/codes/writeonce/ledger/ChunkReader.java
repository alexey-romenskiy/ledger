package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import static java.util.Objects.requireNonNull;

public class ChunkReader {

    private static final int MAX_CHUNK_DATA_SIZE = 0x3fff;

    private final CRC32 checksum = new CRC32();

    private final int capacity;

    @Nonnull
    private final Receiver receiver;

    public ChunkReader(int capacity, @Nonnull Receiver receiver) {

        if (capacity < 23) {
            throw new IllegalArgumentException();
        }

        this.capacity = capacity;
        this.receiver = requireNonNull(receiver);
    }

    public void next(@Nonnull ByteBuffer byteBuffer) {

        if (byteBuffer.capacity() != capacity) {
            throw new IllegalArgumentException();
        }

        byteBuffer.clear();
        var position = 0;
        var remaining = capacity;

        while (remaining >= 14) {
            final var sequence = byteBuffer.getLong(position);
            if (sequence == 0) {
                break;
            }
            if (sequence == Long.MIN_VALUE) {
                throw new IllegalArgumentException();
            }
            final var offset = byteBuffer.getLong(position + 8);
            final var field = byteBuffer.getShort(position + 16);
            final var length = field & MAX_CHUNK_DATA_SIZE;
            final var dataPosition = position + 18;
            final var checksumPosition = dataPosition + length;
            final var storedChecksum = byteBuffer.getInt(checksumPosition);
            byteBuffer.limit(checksumPosition);
            byteBuffer.position(position);
            checksum.reset();
            checksum.update(byteBuffer);
            if (storedChecksum != (int) checksum.getValue()) {
                throw new IllegalArgumentException();
            }
            byteBuffer.position(dataPosition);
            if (sequence < 0) {
                receiver.next(-sequence, offset, true, byteBuffer);
            } else {
                receiver.next(sequence, offset, false, byteBuffer);
            }
            if (byteBuffer.limit() != checksumPosition) {
                throw new IllegalArgumentException();
            }
            if (byteBuffer.position() != byteBuffer.limit()) {
                throw new IllegalArgumentException();
            }
            byteBuffer.limit(capacity);
            final var chunkSize = 22 + length;
            position += chunkSize;
            remaining -= chunkSize;
        }
    }
}
