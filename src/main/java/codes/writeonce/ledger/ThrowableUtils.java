package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class ThrowableUtils {

    @Nonnull
    public static String str(@Nullable Throwable throwable) {
        return String.valueOf(throwable);
    }

    private ThrowableUtils() {
        // empty
    }
}
