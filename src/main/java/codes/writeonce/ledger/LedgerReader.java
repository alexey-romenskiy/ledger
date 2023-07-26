package codes.writeonce.ledger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import static codes.writeonce.ledger.ThrowableUtils.str;

public class LedgerReader implements AutoCloseable {

    private static final AtomicLong READER_THREAD_SEQUENCE = new AtomicLong();

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Nonnull
    private final Slot slot;

    private final long sequence;

    private final long offset;

    @Nonnull
    private final Thread readerThread;

    private volatile boolean shutdown;

    public LedgerReader(
            long topicId,
            @Nonnull Slot slot,
            long sequence,
            long offset,
            @Nonnull LedgerListener listener
    ) {
        logger.info("READER: slotId={} sequence={} offset={} creating", slot.id, sequence, offset);
        this.slot = slot;
        this.sequence = sequence;
        this.offset = offset;
        readerThread = new Thread(() -> {
            logger.info("READER: slotId={} sequence={} offset={} started", slot.id, sequence, offset);
            try {
                final var messageReader = new MessageReader();
                final var messageReceiver = new MessageReceiverImpl(sequence, offset, listener);
                slot.read(sequence, offset, new ReaderCallback() {

                    @Override
                    public boolean next(@Nonnull ByteBuffer byteBuffer) {
                        messageReader.next(byteBuffer, messageReceiver);
                        return !shutdown;
                    }

                    @Override
                    public boolean end() {
                        return !shutdown;
                    }

                    @Override
                    public boolean wakeup() {
                        return !shutdown;
                    }

                    @Override
                    public void failure() {
                        throw new RuntimeException("Failed to read events");
                    }

                    @Override
                    public void close() {
                        logger.info("READER: slotId={} sequence={} offset={} ledger closed", slot.id, sequence, offset);
                    }
                });
            } catch (Exception e) {
                logger.error("READER: slotId={} sequence={} offset={} failed: {}", slot.id, sequence, offset, str(e),
                        e);
            } finally {
                logger.info("READER: slotId={} sequence={} offset={} finished", slot.id, sequence, offset);
            }
        }, "slot-reader#" + topicId + "#" + slot.id + "-" + READER_THREAD_SEQUENCE.incrementAndGet());
        readerThread.start();
    }

    @Override
    public void close() {

        logger.info("READER: slotId={} sequence={} offset={} closing", slot.id, sequence, offset);

        shutdown = true;
        slot.wakeup();

        try {
            readerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static class MessageReceiverImpl extends AbstractMessageReceiver {

        @Nonnull
        private final LedgerListener listener;

        public MessageReceiverImpl(long neededSequence, long neededOffset, @Nonnull LedgerListener listener) {
            super(neededSequence, neededOffset);
            this.listener = listener;
        }

        @Override
        protected void reset(long n) {
            listener.reset(n);
        }

        @Override
        protected void process(long sequence, long offset, boolean last, @Nonnull ByteBuffer byteBuffer) {
            listener.process(sequence, offset, last, byteBuffer);
        }
    }
}
