package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Optional.ofNullable;

public class LedgerInfo implements LedgerInfoMBean {

    private final long topicId;

    @Nonnull
    private final Ledger ledger;

    public LedgerInfo(long topicId, @Nonnull Ledger ledger) {
        this.topicId = topicId;
        this.ledger = ledger;
        try {
            getPlatformMBeanServer().registerMBean(this, new ObjectName("io.trade-mate.topics:name=" + topicId));
        } catch (InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException |
                 MalformedObjectNameException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getTopicId() {
        return topicId;
    }

    @Override
    public long createSlot() {
        return ledger.createSlot().id;
    }

    @Override
    public long createSlot(long sequence, long offset) {
        return ofNullable(ledger.createSlot(sequence, offset)).map(e -> e.id).orElse(-1L);
    }

    @Override
    public boolean deleteSlot(long id) {
        return ledger.deleteSlot(id);
    }

    @Override
    public long getBlockFilePosition() {
        return ledger.getBlockFilePosition();
    }

    @Override
    public long getBlockFileRemained() {
        return ledger.getBlockFileRemained();
    }

    @Override
    public int getQueuePosition() {
        return ledger.getQueuePosition();
    }

    @Nonnull
    @Override
    public Map<Long, Map<String, Object>> getSlotInfos() {
        return ledger.getSlotInfos();
    }

    @Override
    public int getBlockSize() {
        return ledger.getBlockSize();
    }

    @Override
    public int getQueueSize() {
        return ledger.getQueueSize();
    }

    @Override
    public int getQueueRemained() {
        return ledger.getQueueRemained();
    }

    @Override
    public long getFirstNeededBlockFilePosition() {
        return ledger.getFirstNeededBlockFilePosition();
    }

    @Override
    public long getBlockFileSize() {
        return ledger.getBlockFileSize();
    }

    @Override
    public long getStableSequence() {
        return ledger.getBlockWriter().stableSequence;
    }

    @Override
    public long getStableOffset() {
        return ledger.getBlockWriter().stableOffset;
    }

    @Override
    public long getReadySequence() {
        return ledger.getBlockWriter().readySequence;
    }

    @Override
    public long getReadyOffset() {
        return ledger.getBlockWriter().readyOffset;
    }

    @Nullable
    @Override
    public String getWriteFailure() {

        final var writeFailed = ledger.getBlockWriter().writeFailed;
        if (writeFailed == null) {
            return null;
        }

        final var stringWriter = new StringWriter();
        writeFailed.printStackTrace(new PrintWriter(stringWriter));
        return stringWriter.toString();
    }

    @Override
    public int getUnstablePosition() {
        return ledger.getBlockWriter().getUnstablePosition();
    }

    @Override
    public int getUnstableRemaining() {
        return ledger.getBlockWriter().getUnstableRemaining();
    }

    @Override
    public int getPendingWritesCount() {
        return ledger.getBlockWriter().getPendingWritesCount();
    }
}
