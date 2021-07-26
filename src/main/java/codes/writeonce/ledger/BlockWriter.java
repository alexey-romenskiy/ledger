package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

public interface BlockWriter {

    @Nonnull
    ByteBuffer fullBlock(long sequence, long offset, boolean pending) throws InterruptedException;

    void partialBlock(long sequence, long offset, boolean pending, int end) throws InterruptedException;
}
