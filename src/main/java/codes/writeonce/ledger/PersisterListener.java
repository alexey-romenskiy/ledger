package codes.writeonce.ledger;

public interface PersisterListener {

    void persisted(long sequence, long offset, boolean pending);

    void failed();
}
