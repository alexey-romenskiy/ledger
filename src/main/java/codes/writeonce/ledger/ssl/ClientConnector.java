package codes.writeonce.ledger.ssl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicReference;

import static codes.writeonce.ledger.ThrowableUtils.str;
import static java.nio.channels.SelectionKey.OP_CONNECT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ClientConnector extends AbstractConnector {

    private static final long MILLIS_CORRECTION = MILLISECONDS.toNanos(1) - 1;

    private final AtomicReference<PeerSession> session = new AtomicReference<>();

    @Nonnull
    private final SslContextFactory sslContextFactory;

    @Nonnull
    private final String remoteHost;

    private final int remotePort;

    @Nullable
    private final String bindAddr;

    private final int bindPort;

    private final long connectionIntervalNanos;

    private final Selector selector = Selector.open();

    private final Thread initiatorThread;

    private boolean wasConnected;

    private long lastConnectionTime;

    public ClientConnector(
            @Nonnull String id,
            @Nonnull SessionListenerFactory sessionListenerFactory,
            int sendBufferSize,
            int recvBufferSize,
            @Nonnull SslContextFactory sslContextFactory,
            @Nonnull String remoteHost,
            int remotePort,
            @Nullable String bindAddr,
            int bindPort,
            long connectionIntervalNanos
    ) throws IOException {

        super(id, sessionListenerFactory, sendBufferSize, recvBufferSize);

        this.sslContextFactory = sslContextFactory;
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
        this.bindAddr = bindAddr;
        this.bindPort = bindPort;
        this.connectionIntervalNanos = connectionIntervalNanos;

        initiatorThread = new Thread(this::initiatorWorker, id + "-in");
        initiatorThread.start();
    }

    @Override
    protected void opened(@Nonnull PeerSession session) {
        this.session.set(session);
    }

    @Override
    public void closed(@Nonnull PeerSession session) {
        if (this.session.compareAndSet(session, null)) {
            selector.wakeup();
        }
    }

    @Override
    @Nonnull
    protected SSLEngine createSslEngine() throws Exception {

        final var context = sslContextFactory.createSslContext();
        final var engine = context.createSSLEngine(remoteHost, remotePort);
        engine.setUseClientMode(true);
        return engine;
    }

    private void initiatorWorker() {

        try {
            while (true) {
                waitConnect();
                if (shutdown.get()) {
                    break;
                }
                connect();
                while (!shutdown.get() && session.get() != null) {
                    selector.select();
                }
            }
        } catch (Throwable e) {
            logger.error("Connection initiator thread failed: {}", str(e), e);
        } finally {
            logger.info("Connection initiator thread exited");
        }
    }

    private void connect() {

        SocketChannel channel = null;
        try {
            channel = SocketChannel.open();
            configure(channel);

            if (bindAddr != null) {
                channel.bind(new InetSocketAddress(bindAddr, bindPort));
            }

            final var selectionKey = channel.register(selector, OP_CONNECT);
            try {
                if (!channel.connect(new InetSocketAddress(remoteHost, remotePort))) {
                    do {
                        selector.select();
                        if (shutdown.get()) {
                            channel.close();
                            return;
                        }
                    } while (!channel.finishConnect());
                }
                logger.info("Connection established: {}", channel.getLocalAddress());
                connected(channel);
            } finally {
                selectionKey.cancel();
            }
        } catch (Exception e) {
            logger.error("Failed to connect: {}", str(e), e);
            try {
                if (channel != null) {
                    channel.close();
                }
            } catch (IOException ex) {
                logger.error("Failed to close the connection: {}", str(ex), ex);
            }
        }
    }

    private void waitConnect() throws IOException {

        var now = System.nanoTime();

        if (!wasConnected) {
            wasConnected = true;
            lastConnectionTime = now;
            return;
        }

        while (true) {
            final var connElapsed = now - lastConnectionTime;
            if (connElapsed >= connectionIntervalNanos) {
                lastConnectionTime = now;
                return;
            }
            if (shutdown.get()) {
                return;
            }
            selector.select(NANOSECONDS.toMillis(connectionIntervalNanos - connElapsed + MILLIS_CORRECTION));
            now = System.nanoTime();
        }
    }

    @Override
    protected void shutdownWorker() {

        try {
            logger.info("Closing client connector");

            selector.wakeup();
            initiatorThread.join();

            final var s = session.getAndSet(null);
            if (s != null) {
                s.close();
            }

            selector.close();
            shutdownFuture.complete(null);

            logger.info("Client connector closed");
        } catch (Throwable e) {
            logger.error("Failed to close client connector: {}", str(e), e);
            shutdownFuture.completeExceptionally(e);
        }
    }
}
