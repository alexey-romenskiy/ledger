package codes.writeonce.ledger;

import codes.writeonce.ledger.ssl.SessionControl;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLSession;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

import static codes.writeonce.ledger.MessageWriter.HEADER_SIZE;
import static codes.writeonce.ledger.MessageWriter.TRAILER_SIZE;
import static codes.writeonce.ledger.ReplicationConstants.CHECKPOINT_INTERVAL;
import static codes.writeonce.ledger.ReplicationConstants.CMD_CONSUME;
import static codes.writeonce.ledger.ReplicationConstants.CMD_CONSUME_FAIL;
import static codes.writeonce.ledger.ReplicationConstants.CMD_CONSUME_OK;
import static codes.writeonce.ledger.ReplicationConstants.CMD_HEARTBEAT;
import static codes.writeonce.ledger.ReplicationConstants.CMD_READ;
import static codes.writeonce.ledger.ReplicationConstants.CMD_READ_CHUNK;
import static codes.writeonce.ledger.ReplicationConstants.CMD_READ_FAIL;
import static codes.writeonce.ledger.ReplicationConstants.CMD_READ_OK;
import static codes.writeonce.ledger.ReplicationConstants.COMMAND_BUFFER_SIZE;
import static codes.writeonce.ledger.ReplicationConstants.RECEIVER_BUFFER_SIZE;
import static codes.writeonce.ledger.ThrowableUtils.str;

public class ReplicationClientSession extends AbstractSessionListener {

    private static final int SESSION_STATE_NONE = 0;
    private static final int SESSION_STATE_READ_OK = 1;
    private static final int SESSION_STATE_READ = 2;
    private static final int SESSION_STATE_DATA_LENGTH = 3;
    private static final int SESSION_STATE_DATA = 4;

    private static final int DATA_STATE_CHUNK_HEADER = 0;
    private static final int DATA_STATE_CHUNK = 1;
    private static final int DATA_STATE_CHUNK_TRAILER = 2;
    private static final int DATA_STATE_PADDING = 3;

    private final CRC32 checksum = new CRC32();

    private final ByteBuffer commandByteBuffer = ByteBuffer.allocateDirect(COMMAND_BUFFER_SIZE);

    private final ByteBuffer[] outboundBuffers = new ByteBuffer[]{commandByteBuffer};

    private final HeartbeatCommand heartbeatCommand = new HeartbeatCommand();
    private final ConsumeCommand consumeCommand = new ConsumeCommand();
    private final ReadCommand readCommand = new ReadCommand();

    private final long topicId;

    private final long slotId;

    @Nonnull
    private final ReplicationListener listener;

    private final Thread checkpointerThread;

    private final AtomicBoolean shutdown = new AtomicBoolean();

    private final Lock lock = new ReentrantLock();

    private final Condition condition = lock.newCondition();

    private volatile boolean positionDirty = true;

    private final AtomicBoolean positionPending = new AtomicBoolean();

    private final AtomicBoolean readCommandSent = new AtomicBoolean();

    @Nonnull
    private final Position lastPosition;

    private long stableSequence;

    private long stableOffset;

    private int sessionState = SESSION_STATE_NONE;

    private int dataState = DATA_STATE_CHUNK_HEADER;

    private long rawSequence;

    private long dataLength;

    private int dataRead;

    private long offset;

    private int chunkLength;

    private int blockSize;

    private boolean first = true;

    public ReplicationClientSession(
            @Nonnull String id,
            @Nonnull SessionControl sessionControl,
            long topicId,
            long slotId,
            @Nonnull Position lastPosition,
            @Nonnull ReplicationListener listener
    ) {
        super(id, sessionControl, RECEIVER_BUFFER_SIZE);
        this.topicId = topicId;
        this.slotId = slotId;
        this.lastPosition = lastPosition;
        this.listener = listener;

        final var stablePosition = new Position(1, 0);
        listener.readStablePosition(stablePosition);
        this.stableSequence = stablePosition.sequence;
        this.stableOffset = stablePosition.offset;

        checkpointerThread = new Thread(this::checkpointerWorker, id + "-checkpointer");
        checkpointerThread.start();
    }

    @Override
    public void handshakeFinished(@Nonnull SSLSession session) {

        if (positionDirty && !positionPending.getAndSet(true)) {
            submit(consumeCommand);
        }

        if (!readCommandSent.getAndSet(true)) {
            submit(readCommand);
        }
    }

    @Override
    public void closed() {

        if (!shutdown.getAndSet(true)) {
            lock.lock();
            try {
                condition.signalAll();
            } finally {
                lock.unlock();
            }

            try {
                checkpointerThread.join();
            } catch (InterruptedException e) {
                logger.error("REPLCLI: Failed to wait for thread completion: {}", str(e), e);
            }
        }
    }

    @Override
    protected boolean sendData() {
        return true;
    }

    @Override
    public void outboundDone() {
        // empty
    }

    private void checkpointerWorker() {

        try {
            final var position = new Position(stableSequence, stableOffset);
            var now = System.nanoTime();
            var lastCheckpointTime = now;
            while (!shutdown.get()) {
                listener.readStablePosition(position);
                checkpoint(position.sequence, position.offset);
                while (true) {
                    final var elapsed = now - lastCheckpointTime;
                    if (elapsed >= CHECKPOINT_INTERVAL) {
                        lastCheckpointTime = now;
                        break;
                    }
                    lock.lock();
                    try {
                        if (shutdown.get()) {
                            break;
                        }
                        //noinspection ResultOfMethodCallIgnored
                        condition.awaitNanos(CHECKPOINT_INTERVAL - elapsed);
                    } finally {
                        lock.unlock();
                    }
                    now = System.nanoTime();
                }
            }
            logger.info("REPLCLI: Checkpointer thread finished");
        } catch (Throwable e) {
            logger.error("REPLCLI: Checkpointer thread failed: {}", str(e), e);
        }
    }

    private void checkpoint(long sequence, long offset) {

        lock.lock();
        try {
            if (stableSequence == sequence && stableOffset == offset) {
                return;
            }
            stableSequence = sequence;
            stableOffset = offset;
            positionDirty = true;
        } finally {
            lock.unlock();
        }

        if (!positionPending.getAndSet(true)) {
            submit(consumeCommand);
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
                        case CMD_HEARTBEAT -> logger.info("REPLCLI: HEARTBEAT received");
                        case CMD_CONSUME_OK -> consumeOk();
                        case CMD_READ_OK -> {
                            logger.info("REPLCLI: READ command succeeded");
                            sessionState = SESSION_STATE_READ_OK;
                        }
                        case CMD_READ_FAIL -> throw new IllegalArgumentException("READ command failed");
                        case CMD_CONSUME_FAIL -> throw new IllegalArgumentException("CONSUME command failed");
                        default -> throw new IllegalArgumentException("Unexpected command " + cmd);
                    }
                }
                case SESSION_STATE_READ_OK -> {
                    if (inboundBuffer.remaining() < 4) {
                        return;
                    }
                    blockSize = inboundBuffer.getInt();
                    sessionState = SESSION_STATE_READ;
                }
                case SESSION_STATE_READ -> {
                    if (!inboundBuffer.hasRemaining()) {
                        return;
                    }
                    final var cmd = inboundBuffer.get();
                    switch (cmd) {
                        case CMD_HEARTBEAT -> logger.info("REPLCLI: HEARTBEAT received");
                        case CMD_CONSUME_OK -> consumeOk();
                        case CMD_CONSUME_FAIL -> throw new IllegalArgumentException("CONSUME command failed");
                        case CMD_READ_CHUNK -> {
                            logger.info("REPLCLI: CHUNK received");
                            sessionState = SESSION_STATE_DATA_LENGTH;
                        }
                        case CMD_READ_FAIL -> throw new IllegalArgumentException("READ command failed");
                        default -> throw new IllegalArgumentException("Unexpected command " + cmd);
                    }
                }
                case SESSION_STATE_DATA_LENGTH -> {
                    if (inboundBuffer.remaining() < 8) {
                        return;
                    }
                    dataLength = inboundBuffer.getLong();
                    logger.info("REPLCLI: CHUNK received length={}", dataLength);
                    sessionState = SESSION_STATE_DATA;
                }
                case SESSION_STATE_DATA -> {
                    switch (dataState) {
                        case DATA_STATE_CHUNK_HEADER -> {
                            if (dataLength < HEADER_SIZE + TRAILER_SIZE) {
                                throw new IllegalArgumentException();
                            }

                            if (inboundBuffer.remaining() < HEADER_SIZE) {
                                return;
                            }

                            rawSequence = inboundBuffer.getLong();
                            offset = inboundBuffer.getLong();
                            chunkLength = inboundBuffer.getShort() & 0xFFFF;
                            dataRead += HEADER_SIZE;
                            dataLength -= HEADER_SIZE;

                            final var length = chunkLength + TRAILER_SIZE;

                            if (dataRead + length > blockSize) {
                                throw new IllegalArgumentException();
                            }

                            if (dataLength < length) {
                                throw new IllegalArgumentException();
                            }

                            logger.info("REPLCLI: CHUNK sequence={} offset={} last={} length={}", Math.abs(rawSequence),
                                    offset,
                                    rawSequence < 0, chunkLength);

                            final var start = inboundBuffer.position();
                            final var limit = inboundBuffer.limit();

                            inboundBuffer.position(start - HEADER_SIZE);
                            inboundBuffer.limit(start);
                            checksum.reset();
                            checksum.update(inboundBuffer);
                            inboundBuffer.limit(limit);

                            dataState = DATA_STATE_CHUNK;
                        }
                        case DATA_STATE_CHUNK -> {

                            final var length = chunkLength + TRAILER_SIZE;
                            final var start = inboundBuffer.position();
                            final var limit = inboundBuffer.limit();

                            inboundBuffer.limit(Math.min(start + chunkLength, limit));
                            checksum.update(inboundBuffer);

                            final var remaining = limit - start;
                            if (length > remaining) {
                                inboundBuffer.position(start);
                                final var rest = Math.min(remaining, chunkLength);
                                if (rest != 0) {
                                    inboundBuffer.limit(start + rest);
                                    if (rawSequence < 0) {
                                        next(-rawSequence, offset, false);
                                    } else {
                                        next(rawSequence, offset, false);
                                    }

                                    offset += rest;
                                    chunkLength -= rest;
                                    dataRead += rest;
                                    dataLength -= rest;
                                }
                                inboundBuffer.limit(limit);
                                if (chunkLength == 0) {
                                    dataState = DATA_STATE_CHUNK_TRAILER;
                                }
                                return;
                            } else {
                                inboundBuffer.limit(start + length);
                                final var storedChecksum = inboundBuffer.getInt();
                                if (storedChecksum != (int) checksum.getValue()) {
                                    throw new IllegalArgumentException();
                                }
                                inboundBuffer.position(start);
                                inboundBuffer.limit(start + chunkLength);
                                if (rawSequence < 0) {
                                    next(-rawSequence, offset, true);
                                } else {
                                    next(rawSequence, offset, false);
                                }
                                inboundBuffer.limit(limit);
                                inboundBuffer.position(start + length);

                                dataRead += length;
                                dataLength -= length;

                                final var rest = blockSize - dataRead;
                                if (rest < HEADER_SIZE + TRAILER_SIZE) {
                                    if (dataRead == blockSize) {
                                        dataState = DATA_STATE_CHUNK_HEADER;
                                        dataRead = 0;
                                    } else {
                                        dataState = DATA_STATE_PADDING;
                                    }
                                } else {
                                    dataState = DATA_STATE_CHUNK_HEADER;
                                }

                                if (dataLength == 0) {
                                    sessionState = SESSION_STATE_READ;
                                }
                            }
                        }
                        case DATA_STATE_CHUNK_TRAILER -> {
                            if (inboundBuffer.remaining() < TRAILER_SIZE) {
                                return;
                            }

                            final var storedChecksum = inboundBuffer.getInt();
                            if (storedChecksum != (int) checksum.getValue()) {
                                throw new IllegalArgumentException();
                            }

                            if (rawSequence < 0) {
                                final var limit = inboundBuffer.limit();
                                inboundBuffer.limit(inboundBuffer.position());
                                next(-rawSequence, offset, true);
                                inboundBuffer.limit(limit);
                            }

                            dataRead += TRAILER_SIZE;
                            dataLength -= TRAILER_SIZE;

                            final var rest = blockSize - dataRead;
                            if (rest < HEADER_SIZE + TRAILER_SIZE) {
                                if (dataRead == blockSize) {
                                    dataState = DATA_STATE_CHUNK_HEADER;
                                    dataRead = 0;
                                } else {
                                    dataState = DATA_STATE_PADDING;
                                }
                            } else {
                                dataState = DATA_STATE_CHUNK_HEADER;
                            }

                            if (dataLength == 0) {
                                sessionState = SESSION_STATE_READ;
                            }
                        }
                        case DATA_STATE_PADDING -> {
                            if (!inboundBuffer.hasRemaining()) {
                                return;
                            }

                            final var padding = blockSize - dataRead;
                            if (dataLength < padding) {
                                throw new IllegalArgumentException();
                            }

                            final var length = Math.min(padding, inboundBuffer.remaining());
                            inboundBuffer.position(inboundBuffer.position() + length);

                            dataRead += length;
                            dataLength -= length;

                            if (dataRead == blockSize) {
                                dataState = DATA_STATE_CHUNK_HEADER;
                                dataRead = 0;
                            }

                            if (dataLength == 0) {
                                sessionState = SESSION_STATE_READ;
                            }
                        }
                    }
                }
                default -> throw new IllegalArgumentException();
            }
        }
    }

    private void consumeOk() {

        logger.info("REPLCLI: CONSUME command succeeded");
        positionPending.set(false);
        if (positionDirty && !positionPending.getAndSet(true)) {
            submit(consumeCommand);
        }
    }

    private void next(long sequence, long offset, boolean last) {

        if (sequence <= 0) {
            throw new IllegalArgumentException();
        }

        if (offset < 0) {
            throw new IllegalArgumentException();
        }

        final var endOffset = offset + inboundBuffer.remaining();

        if (first) {
            if (sequence == lastPosition.sequence) {
                if (offset == lastPosition.offset) {
                    first = false;
                } else if (offset > lastPosition.offset) {
                    throw new IllegalArgumentException();
                } else {
                    if (endOffset < lastPosition.offset) {
                        return;
                    }
                    final var start = (int) (lastPosition.offset - offset);
                    offset = lastPosition.offset;
                    inboundBuffer.position(inboundBuffer.position() + start);
                    first = false;
                }
            } else if (sequence > lastPosition.sequence) {
                if (offset != 0) {
                    throw new IllegalArgumentException();
                }
                listener.reset(sequence - lastPosition.sequence);
                first = false;
            } else {
                return;
            }
        } else {
            if (sequence == lastPosition.sequence) {
                if (offset != lastPosition.offset) {
                    throw new IllegalArgumentException();
                }
            } else if (sequence > lastPosition.sequence) {
                if (offset != 0) {
                    throw new IllegalArgumentException();
                }
                listener.reset(sequence - lastPosition.sequence);
            } else {
                throw new IllegalArgumentException();
            }
        }

        if (last) {
            lastPosition.sequence = sequence + 1;
            lastPosition.offset = 0;
        } else {
            lastPosition.sequence = sequence;
            lastPosition.offset = endOffset;
        }

        listener.process(sequence, offset, last, inboundBuffer);
    }

    private class ConsumeCommand extends Command {

        @Override
        public void send() {

            logger.info("REPLCLI: Sending consume command");
            commandByteBuffer.clear();
            commandByteBuffer.put(CMD_CONSUME);
            commandByteBuffer.putLong(topicId);
            commandByteBuffer.putLong(slotId);

            lock.lock();
            try {
                commandByteBuffer.putLong(stableSequence);
                commandByteBuffer.putLong(stableOffset);
                positionDirty = false;
            } finally {
                lock.unlock();
            }

            commandByteBuffer.flip();
            outboundBuffers[0] = commandByteBuffer;
            sessionControl.outboundReady(outboundBuffers, 0, 1);
        }
    }

    private class ReadCommand extends Command {

        @Override
        public void send() {

            logger.info("REPLCLI: Sending read command");
            commandByteBuffer.clear();
            commandByteBuffer.put(CMD_READ);
            commandByteBuffer.putLong(topicId);
            commandByteBuffer.putLong(slotId);
            commandByteBuffer.putLong(lastPosition.sequence);
            commandByteBuffer.putLong(lastPosition.offset);
            commandByteBuffer.flip();
            outboundBuffers[0] = commandByteBuffer;
            sessionControl.outboundReady(outboundBuffers, 0, 1);
        }
    }

    private class HeartbeatCommand extends Command {

        @Override
        public void send() {

            logger.info("REPLCLI: Sending heartbeat");
            commandByteBuffer.clear();
            commandByteBuffer.put(CMD_HEARTBEAT);
            commandByteBuffer.flip();
            outboundBuffers[0] = commandByteBuffer;
            sessionControl.outboundReady(outboundBuffers, 0, 1);
        }
    }

    @Nonnull
    @Override
    protected Command getHeartbeatCommand() {
        return heartbeatCommand;
    }
}
