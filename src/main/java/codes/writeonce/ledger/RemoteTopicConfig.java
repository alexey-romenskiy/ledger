package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public record RemoteTopicConfig(
        @Nonnull String hostname,
        int port,
        @Nullable String bindAddr,
        int bindPort,
        long topicId,
        long slotId,
        boolean legacy
) {
    // empty
}
