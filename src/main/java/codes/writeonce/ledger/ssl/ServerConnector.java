package codes.writeonce.ledger.ssl;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.concurrent.Future;

import static codes.writeonce.ledger.ThrowableUtils.str;
import static java.nio.channels.SelectionKey.OP_ACCEPT;

public class ServerConnector extends AbstractConnector {

    @Nonnull
    private final SslContextFactory sslContextFactory;

    private final IdentityHashMap<PeerSession, PeerSession> sessions = new IdentityHashMap<>();

    private final Selector selector = Selector.open();

    private final ServerSocketChannel channel;

    private final Thread acceptorThread;

    private final boolean needClientAuth;

    public ServerConnector(
            @Nonnull String id,
            @Nonnull SessionListenerFactory sessionListenerFactory,
            int sendBufferSize,
            int recvBufferSize,
            @Nonnull SslContextFactory sslContextFactory,
            @Nonnull String hostname,
            int port,
            boolean needClientAuth
    ) throws IOException {

        super(id, sessionListenerFactory, sendBufferSize, recvBufferSize);

        this.sslContextFactory = sslContextFactory;
        this.needClientAuth = needClientAuth;

        channel = ServerSocketChannel.open();
        channel.configureBlocking(false);
        channel.bind(new InetSocketAddress(hostname, port));

        acceptorThread = new Thread(this::acceptorWorker, id + "-ac");
        acceptorThread.start();
    }

    private void acceptorWorker() {

        try {
            final var selectionKey = channel.register(selector, OP_ACCEPT);
            try {
                while (!shutdown.get()) {
                    selector.select();
                    if (selectionKey.isAcceptable()) {
                        final var socketChannel = channel.accept();
                        if (socketChannel != null) {
                            logger.info("Connection accepted: {}", socketChannel.getRemoteAddress());
                            configure(socketChannel);
                            connected(socketChannel);
                        }
                    }
                }
            } finally {
                selectionKey.cancel();
            }
        } catch (Throwable e) {
            logger.error("Connection acceptor thread failed: {}", str(e), e);
        } finally {
            logger.info("Connection acceptor thread exited");
        }
    }

    @Override
    protected void opened(@Nonnull PeerSession session) {

        synchronized (sessions) {
            sessions.put(session, session);
        }
    }

    @Override
    public void closed(@Nonnull PeerSession session) {

        synchronized (sessions) {
            sessions.remove(session);
        }
    }

    @Override
    @Nonnull
    protected SSLEngine createSslEngine() throws Exception {

        final var context = sslContextFactory.createSslContext();
        final var engine = context.createSSLEngine();
        engine.setUseClientMode(false);
        engine.setNeedClientAuth(needClientAuth);
        return engine;
    }

    @Override
    protected void shutdownWorker() {

        try {
            logger.info("Closing server connector");

            selector.wakeup();
            acceptorThread.join();

            try {
                channel.close();
            } catch (IOException e) {
                logger.error("Failed to close the connection: {}", str(e), e);
            }

            final ArrayList<Future<?>> futures;
            synchronized (sessions) {
                futures = new ArrayList<>(sessions.size());
                for (final var session : sessions.keySet()) {
                    futures.add(session.shutdown());
                }
            }
            for (final var future : futures) {
                future.get();
            }

            selector.close();
            shutdownFuture.complete(null);

            logger.info("Server connector closed");
        } catch (Throwable e) {
            logger.error("Failed to close server connector: {}", str(e), e);
            shutdownFuture.completeExceptionally(e);
        }
    }
}
