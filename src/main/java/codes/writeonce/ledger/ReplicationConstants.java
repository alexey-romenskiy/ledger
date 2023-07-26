package codes.writeonce.ledger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

class ReplicationConstants {

    public static final long CONNECTION_INTERVAL = SECONDS.toNanos(5);

    public static final long CHECKPOINT_INTERVAL = MINUTES.toNanos(5);

    public static final long HEARTBEAT_TIMEOUT = SECONDS.toNanos(5);

    public static final long HEARTBEAT_INTERVAL = MILLISECONDS.toNanos(250);

    public static final int CLIENT_SNDBUF = 1024;

    public static final int SERVER_SNDBUF = 256 * 1024 * 1024;

    public static final int CLIENT_RCVBUF = SERVER_SNDBUF;

    public static final int SERVER_RCVBUF = CLIENT_SNDBUF;

    public static final int RECEIVER_BUFFER_SIZE = 0x4000;

    public static final int COMMAND_BUFFER_SIZE = 4096;

    public static final int MAX_OUTBOUND_BUFFERS = 0x1000;

    public static final byte CMD_HEARTBEAT = 2;
    public static final byte CMD_CONSUME = 3;
    public static final byte CMD_CONSUME_OK = 4;
    public static final byte CMD_CONSUME_FAIL = 5;
    public static final byte CMD_READ = 6;
    public static final byte CMD_READ_OK = 7;
    public static final byte CMD_READ_FAIL = 8;
    public static final byte CMD_READ_CHUNK = 9;

    public static final long HEARTBEAT_TIMEOUT_MILLIS = NANOSECONDS.toMillis(HEARTBEAT_TIMEOUT);

    public static final long HEARTBEAT_INTERVAL_MILLIS = NANOSECONDS.toMillis(HEARTBEAT_INTERVAL);
}
