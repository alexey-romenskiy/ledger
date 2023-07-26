package codes.writeonce.ledger;

import codes.writeonce.ledger.ssl.ClientConnector;
import codes.writeonce.ledger.ssl.SessionControl;
import codes.writeonce.ledger.ssl.SessionListener;
import codes.writeonce.ledger.ssl.SessionListenerFactory;
import codes.writeonce.ledger.ssl.SslContextFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static codes.writeonce.ledger.ReplicationConstants.CLIENT_RCVBUF;
import static codes.writeonce.ledger.ReplicationConstants.CLIENT_SNDBUF;
import static codes.writeonce.ledger.ReplicationConstants.CONNECTION_INTERVAL;

public class ReplicationClient implements AutoCloseable {

    private static final AtomicLong THREAD_SEQUENCE = new AtomicLong();

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final ClientConnector connector;

    public ReplicationClient(
            @Nonnull String hostname,
            int port,
            @Nullable String bindAddr,
            int bindPort,
            long topicId,
            long slotId,
            long lastSequence,
            long lastOffset,
            @Nonnull ReplicationListener listener
    ) throws IOException {

        final var path = Path.of(System.getProperty("user.home")).resolve(".secret").resolve("private-keystore");

        final var id = "replcli#" + topicId + "#" + slotId + "-" + THREAD_SEQUENCE.incrementAndGet();
        final var sslContextFactory = new SslContextFactoryImpl(
                "TLSv1.3",
                path,
                "changeit",
                "changeit",
                path,
                "changeit"
        );

        final var lastPosition = new Position(lastSequence, lastOffset);

        connector = new ClientConnector(
                id,
                new SessionListenerFactory() {

                    private long seq = 0;

                    @Nonnull
                    @Override
                    public SessionListener createSessionListener(@Nonnull SessionControl sessionControl) {
                        return new ReplicationClientSession(
                                id + "-" + ++seq,
                                sessionControl,
                                topicId,
                                slotId,
                                lastPosition,
                                listener
                        );
                    }
                },
                CLIENT_SNDBUF,
                CLIENT_RCVBUF,
                sslContextFactory,
                hostname,
                port,
                bindAddr,
                bindPort,
                CONNECTION_INTERVAL
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
