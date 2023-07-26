package codes.writeonce.ledger.ssl;

import javax.annotation.Nonnull;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;

public class SslContextFactoryImpl implements SslContextFactory {

    @Nonnull
    private final String protocol;

    @Nonnull
    private final Path keystorePath;

    @Nonnull
    private final String keystorePassword;

    @Nonnull
    private final String keyPassword;

    @Nonnull
    private final Path truststorePath;

    @Nonnull
    private final String truststorePassword;

    public SslContextFactoryImpl(
            @Nonnull String protocol,
            @Nonnull Path keystorePath,
            @Nonnull String keystorePassword,
            @Nonnull String keyPassword,
            @Nonnull Path truststorePath,
            @Nonnull String truststorePassword
    ) {
        this.protocol = protocol;
        this.keystorePath = keystorePath;
        this.keystorePassword = keystorePassword;
        this.keyPassword = keyPassword;
        this.truststorePath = truststorePath;
        this.truststorePassword = truststorePassword;
    }

    @Override
    @Nonnull
    public SSLContext createSslContext() throws Exception {

        final var context = SSLContext.getInstance(protocol);
        final var keyManagers = createKeyManagers();
        final var trustManagers = createTrustManagers();
        context.init(keyManagers, trustManagers, new SecureRandom());
        return context;
    }

    @Nonnull
    private KeyManager[] createKeyManagers() throws Exception {

        final var keyStore = KeyStore.getInstance("JKS");
        try (var in = Files.newInputStream(keystorePath)) {
            keyStore.load(in, this.keystorePassword.toCharArray());
        }
        final var kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, this.keyPassword.toCharArray());
        return kmf.getKeyManagers();
    }

    @Nonnull
    private TrustManager[] createTrustManagers() throws Exception {

        final var trustStore = KeyStore.getInstance("JKS");
        try (var in = Files.newInputStream(truststorePath)) {
            trustStore.load(in, truststorePassword.toCharArray());
        }
        final var trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustFactory.init(trustStore);
        return trustFactory.getTrustManagers();
    }
}
