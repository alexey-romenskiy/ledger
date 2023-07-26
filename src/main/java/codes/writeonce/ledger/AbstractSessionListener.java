package codes.writeonce.ledger;

import codes.writeonce.ledger.ssl.SessionControl;
import codes.writeonce.ledger.ssl.SessionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

import static codes.writeonce.ledger.ReplicationConstants.HEARTBEAT_INTERVAL;
import static codes.writeonce.ledger.ReplicationConstants.HEARTBEAT_TIMEOUT;

public abstract class AbstractSessionListener implements SessionListener {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final LinkedBlockingQueue<Command> commands = new LinkedBlockingQueue<>();

    protected ByteBuffer inboundBuffer;

    private final ByteBuffer[] inboundBuffers = new ByteBuffer[]{inboundBuffer};

    @Nonnull
    private final String id;

    @Nonnull
    protected final SessionControl sessionControl;

    private boolean heartbeatPending;

    public AbstractSessionListener(@Nonnull String id, @Nonnull SessionControl sessionControl, int receiverBufferSize) {
        this.id = id;
        this.sessionControl = sessionControl;
        inboundBuffer = ByteBuffer.allocateDirect(receiverBufferSize);
    }

    @Nonnull
    @Override
    public String getId() {
        return id;
    }

    @Override
    public void endOfInbound() {
        sessionControl.shutdown();
    }

    @Override
    public boolean inboundOverflow(int proposedCapacity) {

        final var buffer = ByteBuffer.allocateDirect(
                inboundBuffer.remaining() < proposedCapacity
                        ? inboundBuffer.position() + proposedCapacity
                        : inboundBuffer.capacity() * 2);

        inboundBuffer.flip();
        buffer.put(inboundBuffer);
        inboundBuffer = buffer;
        inboundBuffers[0] = buffer;
        sessionControl.inboundReady(inboundBuffers, 0, 1);
        return false;
    }

    @Override
    public void inboundDone() {

        inboundBuffer.flip();
        parse();
        inboundBuffer.compact();
    }

    protected void submit(@Nonnull Command command) {

        try {
            commands.put(command);
            sessionControl.getWriterHive().wakeup();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean outboundUnderflow() {

        final var command = commands.poll();
        if (command == null) {
            if (sendData()) {
                if (heartbeatPending) {
                    getHeartbeatCommand().send();
                } else {
                    return true;
                }
            }
        } else {
            command.send();
        }
        heartbeatPending = false;
        return false;
    }

    protected abstract boolean sendData();

    @Override
    public void recvIdle() {
        sessionControl.getReaderTimer().schedule(System.nanoTime() + HEARTBEAT_TIMEOUT);
    }

    @Override
    public void recvActive() {
        sessionControl.getReaderTimer().cancel();
    }

    @Override
    public void sendIdle() {
        if (!heartbeatPending) {
            sessionControl.getWriterTimer().schedule(System.nanoTime() + HEARTBEAT_INTERVAL);
        }
    }

    @Override
    public void sendActive() {
        sessionControl.getWriterTimer().cancel();
        heartbeatPending = false;
    }

    @Override
    public void writeClosing() {
        sessionControl.getWriterTimer().cancel();
        heartbeatPending = false;
    }

    @Override
    public void readTimer() {
        logger.info("Heartbeat timeout, disconnecting");
        sessionControl.forcedShutdown();
    }

    @Override
    public void writeTimer() {
        heartbeatPending = true;
    }

    @Nonnull
    protected abstract Command getHeartbeatCommand();

    protected abstract void parse();

    protected static abstract class Command {

        public abstract void send();
    }
}
