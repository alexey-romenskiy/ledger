package codes.writeonce.ledger;

public enum EmptyLedgerMetrics implements LedgerMetrics {
    INSTANCE;

    @Override
    public void writeDelay(long nanos) {
        // empty
    }
}
