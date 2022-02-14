package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

public interface BlockReceiver {

    void next(@Nonnull ByteBuffer byteBuffer);
}
