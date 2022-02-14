package codes.writeonce.ledger;

import javax.annotation.Nonnull;

public interface Pool<T> {

    @Nonnull
    T acquire();

    void release(@Nonnull T value);
}
