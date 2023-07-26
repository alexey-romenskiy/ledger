package codes.writeonce.ledger.ssl;

import javax.annotation.Nonnull;

public interface SessionListenerFactory {

    @Nonnull
    SessionListener createSessionListener(@Nonnull SessionControl sessionControl);
}
