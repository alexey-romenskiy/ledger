package codes.writeonce.ledger;

import codes.writeonce.ledger.ssl.ServerConnector;
import codes.writeonce.ledger.ssl.SessionControl;
import codes.writeonce.ledger.ssl.SessionListener;
import codes.writeonce.ledger.ssl.SessionListenerFactory;
import codes.writeonce.ledger.ssl.SslContextFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static codes.writeonce.ledger.ReplicationConstants.SERVER_RCVBUF;
import static codes.writeonce.ledger.ReplicationConstants.SERVER_SNDBUF;

public class ReplicationServer implements AutoCloseable {

    private static final AtomicLong THREAD_SEQUENCE = new AtomicLong();

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final ServerConnector connector;

    public ReplicationServer(
            @Nonnull String hostname,
            int port,
            @Nonnull Map<Long, Ledger> topics,
            @Nonnull ReplicationServerMetrics metrics
    ) throws IOException {

        final var path = Path.of(System.getProperty("user.home")).resolve(".secret").resolve("private-keystore");

        final var sslContextFactory = new SslContextFactoryImpl(
                "TLSv1.3",
                path,
                "changeit",
                "changeit",
                path,
                "changeit"
        );

        final var lockedSlots = new HashMap<Long, Set<Long>>();

        final var serverSequence = THREAD_SEQUENCE.incrementAndGet();
        final var id = "replsrv#" + hostname + ":" + port + "-" + serverSequence;
        connector = new ServerConnector(
                id,
                new SessionListenerFactory() {

                    private long seq = 0;

                    @Nonnull
                    @Override
                    public SessionListener createSessionListener(@Nonnull SessionControl sessionControl) {
                        final var sessionSequence = ++seq;
                        return new ReplicationServerSession(
                                id + "-" + sessionSequence,
                                serverSequence,
                                sessionSequence,
                                sessionControl,
                                topics,
                                metrics,
                                lockedSlots
                        );
                    }
                },
                SERVER_SNDBUF,
                SERVER_RCVBUF,
                sslContextFactory,
                hostname,
                port,
                true
        );
    }

    @Nonnull
    public CompletableFuture<Void> shutdown() {
        return connector.shutdown();
    }

    @Override
    public void close() {

        try {
            connector.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
