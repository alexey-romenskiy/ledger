package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import static codes.writeonce.ledger.MessageWriter.HEADER_SIZE;
import static codes.writeonce.ledger.MessageWriter.TRAILER_SIZE;

public class MessageReader {

    private final CRC32 checksum = new CRC32();

    public void next(@Nonnull ByteBuffer byteBuffer, @Nonnull MessageReceiver receiver) {

        final var limit = byteBuffer.limit();
        var position = byteBuffer.position();
        var remaining = limit - position;

        while (remaining >= HEADER_SIZE + TRAILER_SIZE) {
            final var sequence = byteBuffer.getLong(position);
            if (sequence == 0) {
                break;
            }
            if (sequence == Long.MIN_VALUE) {
                throw new IllegalArgumentException();
            }
            final var offset = byteBuffer.getLong(position + 8);
            final var length = byteBuffer.getShort(position + 16) & 0xFFFF;
            final var dataPosition = position + HEADER_SIZE;
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
            byteBuffer.limit(limit);
            final var chunkSize = HEADER_SIZE + TRAILER_SIZE + length;
            position += chunkSize;
            remaining -= chunkSize;
        }

        byteBuffer.position(limit);
    }
}
