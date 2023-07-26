package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

public interface LedgerListener {

    void reset(long n);

    void process(long sequence, long offset, boolean last, @Nonnull ByteBuffer byteBuffer);
}
