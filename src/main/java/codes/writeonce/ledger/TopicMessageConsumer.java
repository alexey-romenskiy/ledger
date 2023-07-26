package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.io.Serializable;

public interface TopicMessageConsumer {

    void accept(long topicId, long sequence, @Nonnull Serializable payload);
}
