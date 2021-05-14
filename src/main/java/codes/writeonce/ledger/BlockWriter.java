package codes.writeonce.ledger;

import javax.annotation.Nonnull;

public interface BlockWriter {

    void fullBlock(long sequence, long offset, boolean pending, @Nonnull BlockBuffer blockBuffer, int remaining)
            throws InterruptedException;

    void partialBlock(long sequence, long offset, boolean pending, @Nonnull BlockBuffer blockBuffer, int end)
            throws InterruptedException;
}
