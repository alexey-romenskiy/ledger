package codes.writeonce.ledger;

import javax.annotation.Nonnull;

public interface Pool<T> {

    @Nonnull
    T borrow() throws InterruptedException;

    void reclaim(@Nonnull T value);
}
