package codes.writeonce.ledger;

public interface WatchListener extends Receiver, Stability {

    void lost();

    void broken();

    void closed();

    void canceled();
}
