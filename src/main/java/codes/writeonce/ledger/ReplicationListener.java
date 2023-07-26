package codes.writeonce.ledger;

import javax.annotation.Nonnull;

public interface ReplicationListener extends LedgerListener {

    void readStablePosition(@Nonnull Position position);
}
