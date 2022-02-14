package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.util.concurrent.locks.LockSupport;

public class WaiterList {

    private WaiterNode head;

    public void await(@Nonnull WaiterNode node) {

        node.waiterThread = Thread.currentThread();
        node.next = head;
        head = node;
    }

    public void wakeup() {

        var item = head;
        if (item != null) {
            while (true) {
                LockSupport.unpark(item.waiterThread);
                final var next = item.next;
                item.waiterThread = null;
                item.next = null;
                if (next == null) {
                    break;
                }
                item = next;
            }
        }
    }
}
