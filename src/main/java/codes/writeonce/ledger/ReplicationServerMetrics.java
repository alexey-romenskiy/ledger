package codes.writeonce.ledger;

public interface ReplicationServerMetrics {

    void readWait(long nanos);

    void writeWait(long nanos);
}
