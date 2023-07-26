package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

public interface MessageReceiver {

    void next(long sequence, long offset, boolean last, @Nonnull ByteBuffer byteBuffer);
}
