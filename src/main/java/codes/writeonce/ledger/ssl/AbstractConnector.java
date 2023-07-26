package codes.writeonce.ledger.ssl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.SO_SNDBUF;
import static java.net.StandardSocketOptions.TCP_NODELAY;

public abstract class AbstractConnector implements Connector {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected final AtomicBoolean shutdown = new AtomicBoolean();

    protected final CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();

    @Nonnull
    private final SessionListenerFactory sessionListenerFactory;

    private final int sendBufferSize;

    private final int recvBufferSize;

    private final Thread shutdownThread;

    public AbstractConnector(
            @Nonnull String id,
            @Nonnull SessionListenerFactory sessionListenerFactory,
            int sendBufferSize,
            int recvBufferSize
    ) {
        this.sessionListenerFactory = sessionListenerFactory;
        this.sendBufferSize = sendBufferSize;
        this.recvBufferSize = recvBufferSize;

        shutdownThread = new Thread(this::shutdownWorker, id + "-sd");
    }

    protected void configure(@Nonnull SocketChannel channel)
            throws IOException {

        channel.setOption(SO_KEEPALIVE, true);
        channel.setOption(TCP_NODELAY, true);
        channel.setOption(SO_SNDBUF, sendBufferSize);
        channel.setOption(SO_RCVBUF, recvBufferSize);
        channel.configureBlocking(false);
    }

    protected void connected(@Nonnull SocketChannel channel) throws Exception {

        final var session = new PeerSession(createSslEngine(), channel, this, sessionListenerFactory);
        opened(session);
        session.initialize();
    }

    @Override
    public void close() throws ExecutionException, InterruptedException {
        shutdown().get();
    }

    @Nonnull
    public CompletableFuture<Void> shutdown() {

        if (!shutdown.getAndSet(true)) {
            shutdownThread.start();
        }

        return shutdownFuture;
    }

    protected abstract void opened(@Nonnull PeerSession session);

    protected abstract SSLEngine createSslEngine() throws Exception;

    protected abstract void shutdownWorker();
}
