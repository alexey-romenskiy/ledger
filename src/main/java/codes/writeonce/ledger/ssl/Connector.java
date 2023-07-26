package codes.writeonce.ledger.ssl;

import javax.annotation.Nonnull;

interface Connector extends AutoCloseable {

    void closed(@Nonnull PeerSession session);
}
