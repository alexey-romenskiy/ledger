package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

public interface BlockWriter {

    /**
     * @param messageSequence          last chunk message sequence
     * @param accumulatedMessageLength last chunk data end's offset within the message
     * @return next buffer to fill
     */
    @Nonnull
    ByteBuffer fullBlock(long messageSequence, long accumulatedMessageLength);

    /**
     * @param messageSequence          last chunk message sequence
     * @param accumulatedMessageLength last chunk data end's offset within the message
     * @param dataSize                 data size in the buffer
     */
    void partialBlock(long messageSequence, long accumulatedMessageLength, int dataSize);
}
