package codes.writeonce.ledger.ssl;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLSession;

public interface SessionListener {

    @Nonnull
    String getId();

    void handshakeFinished(@Nonnull SSLSession session);

    void closed();

    void endOfInbound();

    boolean inboundOverflow(int proposedCapacity);

    void inboundDone();

    boolean outboundUnderflow();

    void outboundDone();

    void recvIdle();

    void recvActive();

    void sendIdle();

    void sendActive();

    void writeClosing();

    void readTimer();

    void writeTimer();
}
