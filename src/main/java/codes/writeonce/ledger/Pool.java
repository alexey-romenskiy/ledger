package codes.writeonce.ledger;

import javax.annotation.Nonnull;

public interface Pool<T> {

    @Nonnull
    T borrow();

    void reclaim(@Nonnull T value);
}
