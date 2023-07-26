package codes.writeonce.ledger.ssl;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import java.security.SecureRandom;

public class DefaultSslContextFactory implements SslContextFactory {

    @Nonnull
    private final String protocol;

    public DefaultSslContextFactory(@Nonnull String protocol) {
        this.protocol = protocol;
    }

    @Override
    @Nonnull
    public SSLContext createSslContext() throws Exception {

        final var context = SSLContext.getInstance(protocol);
        context.init(null, null, new SecureRandom());
        return context;
    }
}
