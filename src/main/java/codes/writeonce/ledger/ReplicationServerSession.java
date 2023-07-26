package codes.writeonce.ledger;

import codes.writeonce.ledger.ssl.SessionControl;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLSession;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static codes.writeonce.ledger.ReplicationConstants.CMD_CONSUME;
import static codes.writeonce.ledger.ReplicationConstants.CMD_CONSUME_FAIL;
import static codes.writeonce.ledger.ReplicationConstants.CMD_CONSUME_OK;
import static codes.writeonce.ledger.ReplicationConstants.CMD_HEARTBEAT;
import static codes.writeonce.ledger.ReplicationConstants.CMD_READ;
import static codes.writeonce.ledger.ReplicationConstants.CMD_READ_CHUNK;
import static codes.writeonce.ledger.ReplicationConstants.CMD_READ_FAIL;
import static codes.writeonce.ledger.ReplicationConstants.CMD_READ_OK;
import static codes.writeonce.ledger.ReplicationConstants.COMMAND_BUFFER_SIZE;
import static codes.writeonce.ledger.ReplicationConstants.MAX_OUTBOUND_BUFFERS;
import static codes.writeonce.ledger.ThrowableUtils.str;

public class ReplicationServerSession extends AbstractSessionListener {

    private static final int SESSION_STATE_NONE = 0;
    private static final int SESSION_STATE_CONSUME = 1;
    private static final int SESSION_STATE_READ = 2;

    private static final AtomicLong BEE_SEQUENCE = new AtomicLong();

    private final HeartbeatCommand heartbeatCommand = new HeartbeatCommand();
    private final ConsumeOkCommand consumeOkCommand = new ConsumeOkCommand();
    private final ConsumeErrorCommand consumeErrorCommand = new ConsumeErrorCommand();
    private final ReadOkCommand readOkCommand = new ReadOkCommand();
    private final ReadErrorCommand readErrorCommand = new ReadErrorCommand();

    private final long serverSequence;

    private final long sessionSequence;

    @Nonnull
    private final Map<Long, Ledger> topics;

    @Nonnull
    private final ReplicationServerMetrics metrics;

    @Nonnull
    private final Map<Long, Set<Long>> lockedSlots;

    private final ExecutorService commandExecutor;

    private final ByteBuffer commandByteBuffer = ByteBuffer.allocateDirect(COMMAND_BUFFER_SIZE);

    private final ByteBuffer[] outboundBuffers = new ByteBuffer[MAX_OUTBOUND_BUFFERS];

    private int outboundBuffersPosition;

    private int outboundBuffersCount;

    private int lastBufferPosition;

    private boolean dataMode;

    long activeSlotId;

    long activeTopicId;

    private int sessionState = SESSION_STATE_NONE;

    private final AtomicReference<Slot.Cursor> cursor = new AtomicReference<>();

    private int blockSize;

    public ReplicationServerSession(
            @Nonnull String id,
            long serverSequence,
            long sessionSequence,
            @Nonnull SessionControl sessionControl,
            @Nonnull Map<Long, Ledger> topics,
            @Nonnull ReplicationServerMetrics metrics,
            @Nonnull Map<Long, Set<Long>> lockedSlots
    ) {
        super(id, sessionControl, COMMAND_BUFFER_SIZE);
        this.serverSequence = serverSequence;
        this.sessionSequence = sessionSequence;
        this.topics = topics;
        this.metrics = metrics;
        this.lockedSlots = lockedSlots;

        commandExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, id + "-commands"));
    }

    @Override
    public void handshakeFinished(@Nonnull SSLSession session) {
        // empty
    }

    @Override
    public void closed() {

        final var c = cursor.getAndSet(null);
        if (c != null) {
            c.close();
        }

        commandExecutor.shutdown();
        try {
            while (true) {
                if (commandExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        synchronized (lockedSlots) {
            final var slotId = activeSlotId;
            if (slotId != 0) {
                lockedSlots.get(activeTopicId).remove(slotId);
                logger.info("Slot unlocked: topicId={} slotId={}", activeTopicId, slotId);
            }
        }
    }

    private boolean slotLocked(long topicId, long slotId) {

        synchronized (lockedSlots) {
            final var b = lockedSlots.computeIfAbsent(topicId, k -> new HashSet<>()).add(slotId);
            if (b) {
                activeSlotId = slotId;
                activeTopicId = topicId;
                logger.info("Slot locked: topicId={} slotId={}", topicId, slotId);
            }
            return b;
        }
    }

    @Override
    protected void parse() {

        while (true) {
            switch (sessionState) {
                case SESSION_STATE_NONE -> {
                    if (!inboundBuffer.hasRemaining()) {
                        return;
                    }
                    final var cmd = inboundBuffer.get();
                    switch (cmd) {
                        case CMD_HEARTBEAT -> logger.info("REPLSRV: HEARTBEAT received");
                        case CMD_CONSUME -> {
                            logger.info("REPLSRV: CONSUME command received");
                            sessionState = SESSION_STATE_CONSUME;
                        }
                        case CMD_READ -> {
                            logger.info("REPLSRV: READ command received");
                            sessionState = SESSION_STATE_READ;
                        }
                        default -> throw new IllegalArgumentException("Unexpected command " + cmd);
                    }
                }
                case SESSION_STATE_CONSUME -> {
                    if (inboundBuffer.remaining() < 32) {
                        return;
                    }
                    final var topicId = inboundBuffer.getLong();
                    final var slotId = inboundBuffer.getLong();
                    final var sequence = inboundBuffer.getLong();
                    final var offset = inboundBuffer.getLong();
                    sessionState = SESSION_STATE_NONE;
                    consume(topicId, slotId, sequence, offset);
                }
                case SESSION_STATE_READ -> {
                    if (inboundBuffer.remaining() < 32) {
                        return;
                    }
                    final var topicId = inboundBuffer.getLong();
                    final var slotId = inboundBuffer.getLong();
                    final var sequence = inboundBuffer.getLong();
                    final var offset = inboundBuffer.getLong();
                    sessionState = SESSION_STATE_NONE;
                    read(topicId, slotId, sequence, offset);
                }
                default -> throw new IllegalArgumentException();
            }
        }
    }

    @Override
    public void outboundDone() {

        if (dataMode) {
            final var c = cursor.get();
            if (c != null) {
                final var length = outboundBuffersCount + 1;
                var n = outboundBuffersPosition;
                if (n < length) {
                    do {
                        if (outboundBuffers[n].hasRemaining()) {
                            break;
                        }
                        n++;
                    } while (n < length);
                    if (n != outboundBuffersPosition) {
                        var shift = n - outboundBuffersPosition;
                        if (outboundBuffersPosition == 0) {
                            shift--;
                        }
                        if (shift != 0) {
                            if (n == length && lastBufferPosition != c.blockSize()) {
                                shift--;
                            }
                            if (shift != 0) {
                                c.skip(shift);
                                c.prefetch();
                            }
                        }
                        outboundBuffersPosition = n;
                    }
                }
            }
        }
    }

    private void consume(long topicId, long slotId, long sequence, long offset) {

        logger.info("REPLSRV: Topic id={} slot id={} consume sequence={} offset={}", topicId, slotId, sequence, offset);
        commandExecutor.execute(() -> {
            try {
                final var ledger = topics.get(topicId);
                if (ledger == null) {
                    logger.error(
                            "REPLSRV: Topic id={} slot id={} consume sequence={} offset={} failed: topic not found",
                            topicId, slotId, sequence, offset);
                    submit(consumeErrorCommand);
                } else {
                    final var slot = ledger.slot(slotId);
                    if (slot == null) {
                        logger.error(
                                "REPLSRV: Topic id={} slot id={} consume sequence={} offset={} failed: slot not found",
                                topicId, slotId, sequence, offset);
                        submit(consumeErrorCommand);
                    } else {
                        try {
                            logger.info("REPLSRV: Topic id={} slot id={} consume sequence={} offset={} calling",
                                    topicId, slotId, sequence, offset);
                            slot.consume(sequence, offset);
                            logger.info("REPLSRV: Topic id={} slot id={} consume sequence={} offset={} succeeded",
                                    topicId, slotId, sequence, offset);
                            submit(consumeOkCommand);
                        } catch (Exception e) {
                            logger.error("REPLSRV: Topic id={} slot id={} consume sequence={} offset={} failed: {}",
                                    topicId, slotId, sequence, offset, str(e), e);
                            submit(consumeErrorCommand);
                        }
                    }
                }
            } catch (Throwable e) {
                logger.error("REPLSRV: Topic id={} slot id={} consume sequence={} offset={} failed: {}", topicId,
                        slotId, sequence, offset, str(e), e);
            }
        });
    }

    private void read(long topicId, long slotId, long sequence, long offset) {

        logger.info("REPLSRV: Topic id={} slot id={} read sequence={} offset={}", topicId, slotId, sequence, offset);
        commandExecutor.execute(() -> {
            try {
                if (activeSlotId != 0) {
                    logger.error(
                            "REPLSRV: Topic id={} slot id={} read sequence={} offset={} failed: already selected topic id={} slot id={}",
                            topicId, slotId, sequence, offset, activeTopicId, activeSlotId);
                    submit(readErrorCommand);
                    return;
                }

                final var ledger = topics.get(topicId);
                if (ledger == null) {
                    logger.error("REPLSRV: Topic id={} slot id={} read sequence={} offset={} failed: topic not found",
                            topicId,
                            slotId, sequence, offset);
                    submit(readErrorCommand);
                    return;
                }

                final var slot = ledger.slot(slotId);
                if (slot == null) {
                    logger.error("REPLSRV: Topic id={} slot id={} read sequence={} offset={} failed: slot not found",
                            topicId,
                            slotId, sequence, offset);
                    submit(readErrorCommand);
                    return;
                }

                if (!slotLocked(topicId, slotId)) {
                    logger.error("REPLSRV: Topic id={} slot id={} read sequence={} offset={} failed: already locked",
                            topicId,
                            slotId, sequence, offset);
                    submit(readErrorCommand);
                    return;
                }

                final var c = slot.cursor(sequence, offset);
                if (c == null) {
                    submit(readErrorCommand);
                    return;
                }

                try {
                    sessionControl.changeId(
                            "replsrv#" + topicId + "#" + slotId + "-" + serverSequence + "-" + sessionSequence);

                    blockSize = c.blockSize();
                    submit(readOkCommand);
                } catch (Throwable e) {
                    c.close();
                    throw e;
                }

                cursor.set(c);

                sessionControl.getWriterHive().wakeup();
            } catch (Throwable e) {
                logger.error("REPLSRV: read failed: {}", str(e), e);
                submit(readErrorCommand);
            } finally {
                logger.info("REPLSRV: read finished");
            }
        });
    }

    private class HeartbeatCommand extends Command {

        @Override
        public void send() {

            logger.info("REPLSRV: Actually sending heartbeat");
            commandByteBuffer.clear();
            commandByteBuffer.put(CMD_HEARTBEAT);
            commandByteBuffer.flip();
            outboundBuffers[0] = commandByteBuffer;
            dataMode = false;
            sessionControl.outboundReady(outboundBuffers, 0, 1);
        }
    }

    private class ConsumeOkCommand extends Command {

        @Override
        public void send() {

            logger.info("REPLSRV: Actually sending consume ok");
            commandByteBuffer.clear();
            commandByteBuffer.put(CMD_CONSUME_OK);
            commandByteBuffer.flip();
            outboundBuffers[0] = commandByteBuffer;
            dataMode = false;
            sessionControl.outboundReady(outboundBuffers, 0, 1);
        }
    }

    private class ConsumeErrorCommand extends Command {

        @Override
        public void send() {

            logger.info("REPLSRV: Actually sending consume error");
            commandByteBuffer.clear();
            commandByteBuffer.put(CMD_CONSUME_FAIL);
            commandByteBuffer.flip();
            outboundBuffers[0] = commandByteBuffer;
            dataMode = false;
            sessionControl.outboundReady(outboundBuffers, 0, 1);
        }
    }

    private class ReadOkCommand extends Command {

        @Override
        public void send() {

            logger.info("REPLSRV: Actually sending read ok");
            commandByteBuffer.clear();
            commandByteBuffer.put(CMD_READ_OK);
            commandByteBuffer.putInt(blockSize);
            commandByteBuffer.flip();
            outboundBuffers[0] = commandByteBuffer;
            dataMode = false;
            sessionControl.outboundReady(outboundBuffers, 0, 1);
        }
    }

    private class ReadErrorCommand extends Command {

        @Override
        public void send() {

            logger.info("REPLSRV: Actually sending read error");
            commandByteBuffer.clear();
            commandByteBuffer.put(CMD_READ_FAIL);
            commandByteBuffer.flip();
            outboundBuffers[0] = commandByteBuffer;
            dataMode = false;
            sessionControl.outboundReady(outboundBuffers, 0, 1);
        }
    }

    @Override
    protected boolean sendData() {

        final var c = cursor.get();
        if (c == null) {
            return true;
        }

        final var hive = sessionControl.getWriterHive();

        while (true) {
            final var bee = c.prefetch(hive,
                    "prefetch-" + activeTopicId + "-" + activeSlotId + "-" + BEE_SEQUENCE.incrementAndGet());
            if (bee != null) {
                logger.info("Bee created: {}", bee.value());
                return true;
            }
            final var n = c.next(0, outboundBuffers, 1, outboundBuffers.length - 1);
            final var p = lastBufferPosition == c.blockSize() ? 0 : lastBufferPosition;
            if (n > 0) {
                outboundBuffers[1].position(p);
                final var length = n + 1;
                long size = 0;
                for (var i = 1; i < length; i++) {
                    size += outboundBuffers[i].remaining();
                }
                if (size > 0) {
                    logger.info("REPLSRV: Actually sending data size={}", size);
                    commandByteBuffer.clear();
                    commandByteBuffer.put(CMD_READ_CHUNK);
                    commandByteBuffer.putLong(size);
                    commandByteBuffer.flip();
                    outboundBuffers[0] = commandByteBuffer;
                    lastBufferPosition = outboundBuffers[n].limit();
                    outboundBuffersCount = n;
                    outboundBuffersPosition = 0;
                    dataMode = true;
                    sessionControl.outboundReady(outboundBuffers, 0, length);
                    return false;
                }
            }
            final var b = c.next(0, p, hive,
                    "update-" + activeTopicId + "-" + activeSlotId + "-" + BEE_SEQUENCE.incrementAndGet());
            if (b != null) {
                logger.info("Bee created: {}", b.value());
                return true;
            }
        }
    }

    @Nonnull
    @Override
    protected Command getHeartbeatCommand() {
        return heartbeatCommand;
    }
}
