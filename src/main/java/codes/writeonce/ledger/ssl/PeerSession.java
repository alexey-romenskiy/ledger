package codes.writeonce.ledger.ssl;

import codes.writeonce.concurrency.HiveTimer;
import codes.writeonce.concurrency.NioHive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static codes.writeonce.ledger.ThrowableUtils.str;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;
import static java.util.Objects.requireNonNull;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.FINISHED;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_UNWRAP;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_WRAP;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;

public class PeerSession implements SessionControl {

    private static final ByteBuffer[] EMPTY_BUFFERS = new ByteBuffer[0];

    private static final int INITIAL_BUFFER_SIZE = 0x10000;

    private static final int BUFFER_WATERMARK = 0x4000;

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final AtomicBoolean shutdown = new AtomicBoolean();

    private final AtomicBoolean needWrap = new AtomicBoolean();

    private final AtomicBoolean needUnwrap = new AtomicBoolean();

    private final AtomicInteger threadCount = new AtomicInteger(2);

    private final CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();

    @Nonnull
    private final SSLEngine engine;

    @Nonnull
    private final SocketChannel channel;

    @Nonnull
    private final Connector connector;

    @Nonnull
    private final SessionListenerFactory sessionListenerFactory;

    private volatile boolean readerFailed;

    private volatile boolean writerFailed;

    private Selector readerSelector;

    private Selector writerSelector;

    private HiveTimer readerTimer;

    private HiveTimer writerTimer;

    private NioHive<String> readerHive;

    private NioHive<String> writerHive;

    private Thread readerThread;

    private Thread writerThread;

    private Thread shutdownThread;

    private SessionListener sessionListener;

    private ByteBuffer recvBuffer;

    private ByteBuffer sendBuffer;

    private ByteBuffer[] inboundBuffers = EMPTY_BUFFERS;

    private int inboundBuffersOffset;

    private int inboundBuffersLength;

    private ByteBuffer[] outboundBuffers = EMPTY_BUFFERS;

    private int outboundBuffersOffset;

    private int outboundBuffersLength;

    public PeerSession(
            @Nonnull SSLEngine engine,
            @Nonnull SocketChannel channel,
            @Nonnull Connector connector,
            @Nonnull SessionListenerFactory sessionListenerFactory
    ) {
        this.engine = engine;
        this.channel = channel;
        this.connector = connector;
        this.sessionListenerFactory = sessionListenerFactory;
    }

    public void initialize() {
        try {
            readerSelector = Selector.open();
            writerSelector = Selector.open();

            readerHive = new NioHive<>(readerSelector);
            writerHive = new NioHive<>(writerSelector);
            readerTimer = new HiveTimer(readerHive);
            writerTimer = new HiveTimer(writerHive);

            engine.beginHandshake();

            final var initialPacketBufferSize = engine.getSession().getPacketBufferSize();
            final var initialApplicationBufferSize = engine.getSession().getApplicationBufferSize();

            /*logger.info("Init initialPacketBufferSize={} initialApplicationBufferSize={}", initialPacketBufferSize,
                    initialApplicationBufferSize);*/

            final var encryptedBufferSize = Math.max(INITIAL_BUFFER_SIZE, initialPacketBufferSize);

            recvBuffer = ByteBuffer.allocateDirect(encryptedBufferSize);
            sendBuffer = ByteBuffer.allocateDirect(encryptedBufferSize);

            sessionListener = sessionListenerFactory.createSessionListener(this);

            final var id = sessionListener.getId();
            readerThread = new Thread(this::readerWorker, id + "-rd");
            writerThread = new Thread(this::writerWorker, id + "-wr");
            shutdownThread = new Thread(this::shutdownWorker, id + "-sd");

            logger.info("Session opened: {}", id);

            readerThread.start();
            writerThread.start();
        } catch (Exception e) {
            logger.error("Failed to initialize the connection: {}", str(e), e);
            shutdown.set(true);
            try {
                channel.close();
            } catch (IOException ex) {
                logger.error("Failed to close the connection: {}", str(ex), ex);
            }
            connector.closed(this);
            if (sessionListener != null) {
                sessionListener.closed();
            }
            shutdownFuture.complete(null);
        }
    }

    @Override
    public void changeId(@Nonnull String id) {

        readerThread.setName(id + "-rd");
        writerThread.setName(id + "-wr");
        shutdownThread.setName(id + "-sd");
    }

    @Override
    public void close() throws ExecutionException, InterruptedException {
        shutdown().get();
    }

    @Override
    public void forcedShutdown() {

        readerFailed = true;
        writerFailed = true;
        readerSelector.wakeup();
        writerSelector.wakeup();
    }

    @Override
    @Nonnull
    public CompletableFuture<Void> shutdown() {

        if (!shutdown.getAndSet(true)) {
            shutdownThread.start();
        }

        return shutdownFuture;
    }

    private void readerWorker() {

        try {
            final var selectionKey = channel.register(readerSelector, OP_READ);
            try {
                var handshakeStatus = engine.getHandshakeStatus();
                var idle = false;

                while (!writerFailed) {
                    drainBees(readerHive);
                    handshakeStatus = readerLoop(handshakeStatus);
                    if (handshakeStatus == null) {
                        // normal disconnection
                        break;
                    }
                    if (recvBuffer.hasRemaining()) {
                        final var length = channel.read(recvBuffer);
                        //logger.info("Read {}", length);
                        if (length == -1) {
                            // abnormal disconnection
                            break;
                        } else if (length == 0) {
                            if (!idle) {
                                idle = true;
                                sessionListener.recvIdle();
                            }
                            //logger.info("select read");
                            if (readerTimer.select()) {
                                sessionListener.readTimer();
                            }
                            //logger.info("select read awaken");
                            handshakeStatus = engine.getHandshakeStatus();
                        } else {
                            if (idle) {
                                idle = false;
                                sessionListener.recvActive();
                            }
                        }
                    } else {
                        selectionKey.interestOps(0);
                        //logger.info("select read overflow");
                        if (readerTimer.select()) {
                            sessionListener.readTimer();
                        }
                        //logger.info("select read overflow awaken");
                        selectionKey.interestOps(OP_READ);
                        handshakeStatus = engine.getHandshakeStatus();
                    }
                }
            } finally {
                selectionKey.cancel();
                sessionListener.endOfInbound();
            }
        } catch (Throwable e) {
            logger.error("Reader thread failed: {}", str(e), e);
            readerFailed = true;
            //logger.info("wakeup write");
            writerSelector.wakeup();
        } finally {
            drainBees(readerHive);
            decrement();
            logger.info("Reader thread exited");
        }
    }

    private void writerWorker() {

        try {
            final var selectionKey = channel.register(writerSelector, OP_WRITE);
            try {
                sendBuffer.flip();
                var handshakeStatus = engine.getHandshakeStatus();
                var idle = false;

                while (!readerFailed) {
                    drainBees(writerHive);
                    if (sendBuffer.remaining() < BUFFER_WATERMARK) {
                        handshakeStatus = writerLoop(handshakeStatus);
                        if (handshakeStatus == null) {
                            break;
                        }
                    }
                    if (sendBuffer.hasRemaining()) {
                        final var length = channel.write(sendBuffer);
                        //logger.info("Write {}", length);
                        if (length == 0) {
                            //logger.info("select write");
                            if (writerTimer.select()) {
                                sessionListener.writeTimer();
                            }
                            //logger.info("select write awaken");
                            handshakeStatus = engine.getHandshakeStatus();
                        } else if (idle) {
                            idle = false;
                            sessionListener.sendActive();
                        }
                    } else {
                        if (!idle) {
                            idle = true;
                            sessionListener.sendIdle();
                        }
                        selectionKey.interestOps(0);
                        //logger.info("select write nothing");
                        if (writerTimer.select()) {
                            sessionListener.writeTimer();
                        }
                        //logger.info("select write nothing awaken");
                        selectionKey.interestOps(OP_WRITE);
                        handshakeStatus = engine.getHandshakeStatus();
                    }
                }

                sessionListener.writeClosing();

                while (sendBuffer.hasRemaining()) {
                    drainBees(writerHive);
                    final var length = channel.write(sendBuffer);
                    if (length == 0) {
                        //logger.info("select write 2");
                        if (writerTimer.select()) {
                            sessionListener.writeTimer();
                        }
                        //logger.info("select write 2 awaken");
                    }
                }
            } finally {
                selectionKey.cancel();
            }
        } catch (Throwable e) {
            logger.error("Writer thread failed: {}", str(e), e);
            writerFailed = true;
            //logger.info("wakeup read");
            readerSelector.wakeup();
        } finally {
            drainBees(writerHive);
            decrement();
            logger.info("Writer thread exited");
        }
    }

    private void drainBees(@Nonnull NioHive<String> hive) {
        for (var bee = hive.nextBee(); bee != null; bee = hive.nextBee()) {
            logger.info("Bee returned: {}", bee.value());
            bee.close();
        }
    }

    @Nullable
    private HandshakeStatus readerLoop(@Nonnull HandshakeStatus handshakeStatus) throws SSLException {

        recvBuffer.flip();

        while (true) {
            //logger.info("handshakeStatus: {}", handshakeStatus);
            switch (handshakeStatus) {
                case NEED_WRAP -> {
                    if (!needWrap.getAndSet(true)) {
                        //logger.info("wakeup write from reader loop");
                        writerSelector.wakeup();
                    }
                    recvBuffer.compact();
                    return handshakeStatus;
                }
                case NEED_UNWRAP, NOT_HANDSHAKING -> {
                    if (!recvBuffer.hasRemaining()) {
                        recvBuffer.compact();
                        return handshakeStatus;
                    }
                    final var waitingUnwrap = needUnwrap.getAndSet(false);
                    final var result =
                            engine.unwrap(recvBuffer, inboundBuffers, inboundBuffersOffset, inboundBuffersLength);
                    //logger.info("Result: {}", result);
                    handshakeStatus = result.getHandshakeStatus();
                    if (handshakeStatus == NEED_UNWRAP) {
                        needUnwrap.set(true);
                    } else if (waitingUnwrap) {
                        writerSelector.wakeup();
                    }
                    simplifyInbound();
                    if (handshakeStatus == FINISHED) {
                        sessionListener.handshakeFinished(engine.getSession());
                        handshakeStatus = engine.getHandshakeStatus();
                    }
                    final var status = result.getStatus();
                    switch (status) {
                        case OK -> {
                            if (result.bytesProduced() != 0) {
                                sessionListener.inboundDone();
                            }
                        }
                        case BUFFER_OVERFLOW -> {
                            if (handshakeStatus == NOT_HANDSHAKING &&
                                sessionListener.inboundOverflow(engine.getSession().getApplicationBufferSize())) {
                                recvBuffer.compact();
                                return handshakeStatus;
                            }
                        }
                        case BUFFER_UNDERFLOW -> {
                            if (handshakeStatus == NEED_UNWRAP || handshakeStatus == NOT_HANDSHAKING) {
                                recvBuffer.compact();
                                if (!recvBuffer.hasRemaining()) {
                                    recvBuffer = enlargeBuffer(recvBuffer, engine.getSession().getPacketBufferSize());
                                }
                                return handshakeStatus;
                            }
                        }
                        case CLOSED -> {
                            recvBuffer.compact();
                            engine.closeInbound();
                            return null;
                        }
                        default -> throw new IllegalStateException("Unsupported unwrap status: " + status);
                    }
                }
                case NEED_TASK -> {
                    while (true) {
                        final var task = engine.getDelegatedTask();
                        if (task == null) {
                            break;
                        }
                        task.run();
                    }
                    handshakeStatus = engine.getHandshakeStatus();
                }
                default -> throw new IllegalStateException("Unsupported handshake status: " + handshakeStatus);
            }
        }
    }

    @Nullable
    private HandshakeStatus writerLoop(@Nonnull HandshakeStatus handshakeStatus) throws SSLException {

        sendBuffer.compact();

        while (true) {
            //logger.info("handshakeStatus: {}", handshakeStatus);
            switch (handshakeStatus) {
                case NEED_UNWRAP -> {
                    if (!needUnwrap.getAndSet(true)) {
                        //logger.info("wakeup read from writer loop");
                        readerSelector.wakeup();
                    }
                    sendBuffer.flip();
                    return handshakeStatus;
                }
                case NEED_WRAP, NOT_HANDSHAKING -> {
                    final var waitingWrap = needWrap.getAndSet(false);
                    final var result =
                            engine.wrap(outboundBuffers, outboundBuffersOffset, outboundBuffersLength, sendBuffer);
                    //logger.info("Result: {}", result);
                    handshakeStatus = result.getHandshakeStatus();
                    if (handshakeStatus == NEED_WRAP) {
                        needWrap.set(true);
                    } else if (waitingWrap) {
                        readerSelector.wakeup();
                    }
                    simplifyOutbound();
                    if (handshakeStatus == FINISHED) {
                        sessionListener.handshakeFinished(engine.getSession());
                        handshakeStatus = engine.getHandshakeStatus();
                    }
                    final var status = result.getStatus();
                    switch (status) {
                        case OK -> {
                            if (result.bytesConsumed() != 0) {
                                sessionListener.outboundDone();
                            }
                            if (handshakeStatus == NOT_HANDSHAKING && outboundBuffersLength == 0 &&
                                sessionListener.outboundUnderflow()) {
                                sendBuffer.flip();
                                return handshakeStatus;
                            }
                        }
                        case BUFFER_OVERFLOW -> {
                            if (sendBuffer.position() < BUFFER_WATERMARK) {
                                sendBuffer = enlargeBuffer(sendBuffer, engine.getSession().getPacketBufferSize());
                            } else if (handshakeStatus == NEED_WRAP || handshakeStatus == NOT_HANDSHAKING) {
                                sendBuffer.flip();
                                return handshakeStatus;
                            }
                        }
                        case CLOSED -> {
                            sendBuffer.flip();
                            return null;
                        }
                        default -> throw new IllegalStateException("Unsupported wrap status: " + status);
                    }
                }
                case NEED_TASK -> {
                    while (true) {
                        final var task = engine.getDelegatedTask();
                        if (task == null) {
                            break;
                        }
                        task.run();
                    }
                    handshakeStatus = engine.getHandshakeStatus();
                }
                default -> throw new IllegalStateException("Unsupported handshake status: " + handshakeStatus);
            }
        }
    }

    private void decrement() {

        var expectedValue = threadCount.get();
        while (expectedValue > 0) {
            final var newValue = expectedValue - 1;
            final var witnessValue = threadCount.compareAndExchange(expectedValue, newValue);
            if (witnessValue == expectedValue) {
                if (newValue == 0) {
                    shutdown();
                }
                break;
            }
            expectedValue = witnessValue;
        }
    }

    private void shutdownWorker() {
        try {
            logger.info("Closing session");

            engine.closeOutbound();
            readerSelector.wakeup();
            writerSelector.wakeup();

            readerThread.join();
            writerThread.join();

            readerSelector.close();
            writerSelector.close();
            channel.close();

            connector.closed(this);
            sessionListener.closed();
            shutdownFuture.complete(null);

            logger.info("Session closed");
        } catch (Throwable e) {
            logger.error("Failed to close session: {}", str(e), e);
            shutdownFuture.completeExceptionally(e);
        }
    }

    @Nonnull
    protected ByteBuffer enlargeBuffer(@Nonnull ByteBuffer buffer, int sessionProposedCapacity) {

        final var capacity = sessionProposedCapacity > buffer.remaining()
                ? sessionProposedCapacity + buffer.position()
                : buffer.capacity() * 2;

        /*logger.info("Enlarge buffer remaining={} sessionProposedCapacity={} capacity={}", buffer.remaining(),
                sessionProposedCapacity, capacity);*/

        final var replaceBuffer = ByteBuffer.allocateDirect(capacity);

        buffer.flip();
        replaceBuffer.put(buffer);
        return replaceBuffer;
    }

    @Override
    @Nonnull
    public HiveTimer getReaderTimer() {
        return requireNonNull(readerTimer);
    }

    @Override
    @Nonnull
    public HiveTimer getWriterTimer() {
        return requireNonNull(writerTimer);
    }

    @Override
    @Nonnull
    public NioHive<String> getReaderHive() {
        return requireNonNull(readerHive);
    }

    @Override
    @Nonnull
    public NioHive<String> getWriterHive() {
        return requireNonNull(writerHive);
    }

    @Override
    public void outboundReady(@Nonnull ByteBuffer[] buffers, int offset, int length) {
        outboundBuffers = buffers;
        outboundBuffersOffset = offset;
        outboundBuffersLength = length;
        simplifyOutbound();
    }

    @Override
    public void inboundReady(@Nonnull ByteBuffer[] buffers, int offset, int length) {
        inboundBuffers = buffers;
        inboundBuffersOffset = offset;
        inboundBuffersLength = length;
        simplifyInbound();
    }

    private void simplifyInbound() {

        while (inboundBuffersLength > 0) {
            if (inboundBuffers[inboundBuffersOffset].hasRemaining()) {
                return;
            }
            inboundBuffersOffset++;
            inboundBuffersLength--;
        }
        inboundBuffers = EMPTY_BUFFERS;
        inboundBuffersOffset = 0;
        inboundBuffersLength = 0;
    }

    private void simplifyOutbound() {

        while (outboundBuffersLength > 0) {
            if (outboundBuffers[outboundBuffersOffset].hasRemaining()) {
                return;
            }
            outboundBuffersOffset++;
            outboundBuffersLength--;
        }
        outboundBuffers = EMPTY_BUFFERS;
        outboundBuffersOffset = 0;
        outboundBuffersLength = 0;
    }
}
