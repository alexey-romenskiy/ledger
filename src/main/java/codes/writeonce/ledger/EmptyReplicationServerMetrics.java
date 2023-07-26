package codes.writeonce.ledger;

public enum EmptyReplicationServerMetrics implements ReplicationServerMetrics {
    INSTANCE;

    @Override
    public void readWait(long nanos) {
        // empty
    }

    @Override
    public void writeWait(long nanos) {
        // empty
    }
}
