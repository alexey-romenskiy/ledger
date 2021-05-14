package codes.writeonce.ledger;

public interface Stability {

    void stabilized(long sequence, long offset, boolean last);
}
