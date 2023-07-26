package codes.writeonce.ledger;

import java.io.Serial;

public class LedgerException extends Exception {

    @Serial
    private static final long serialVersionUID = 5541587092580873284L;

    public LedgerException(String message) {
        super(message);
    }

    public LedgerException(String message, Throwable cause) {
        super(message, cause);
    }
}
