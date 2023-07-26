package codes.writeonce.ledger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static codes.writeonce.ledger.ReplicationConstants.HEARTBEAT_INTERVAL_MILLIS;
import static codes.writeonce.ledger.ReplicationConstants.HEARTBEAT_TIMEOUT_MILLIS;

public abstract class AbstractReplication implements AutoCloseable {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected long now;
    protected long lastSentTime;
    protected long lastRecvHeartbeatTime;
    protected boolean heartbeatPending;
    protected boolean heartbeatSending;

    protected void updateLastRecvHeartbeatTime(long now) {
        logger.info("REPL: heartbeat delay: {}", now - lastRecvHeartbeatTime);
        lastRecvHeartbeatTime = now;
    }

    protected boolean isHeartbeatRequired() {
        return !heartbeatPending && getSentHeartbeatElapsed() >= HEARTBEAT_INTERVAL_MILLIS;
    }

    protected long getRecvHeartbeatRemaining() {
        return Math.max(HEARTBEAT_TIMEOUT_MILLIS - getRecvHeartbeatElapsed(), 0);
    }

    protected long getSentHeartbeatDelay() {
        return Math.max(HEARTBEAT_INTERVAL_MILLIS - getSentHeartbeatElapsed(), 0);
    }

    protected long getRecvHeartbeatElapsed() {
        return Math.max(now - lastRecvHeartbeatTime, 0);
    }

    protected long getSentHeartbeatElapsed() {
        return Math.max(now - lastSentTime, 0);
    }

    protected long getReadDelay() {
        return Math.min(getSentHeartbeatDelay(), getRecvHeartbeatRemaining());
    }
}
