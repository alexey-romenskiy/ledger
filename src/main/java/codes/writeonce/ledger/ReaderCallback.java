package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

public interface ReaderCallback {

    boolean next(@Nonnull ByteBuffer byteBuffer);

    boolean end();

    boolean wakeup();

    void failure();

    void close();
}
