package codes.writeonce.ledger.ssl;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;

public interface SslContextFactory {

    @Nonnull
    SSLContext createSslContext() throws Exception;
}
