package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

public interface LedgerInfoMBean {

    long getTopicId();

    long createSlot();

    long createSlot(long sequence, long offset);

    boolean deleteSlot(long id);

    long getBlockFilePosition();

    long getBlockFileRemained();

    int getQueuePosition();

    @Nonnull
    Map<Long, Map<String, Object>> getSlotInfos();

    int getBlockSize();

    int getQueueSize();

    int getQueueRemained();

    long getFirstNeededBlockFilePosition();

    long getBlockFileSize();

    long getStableSequence();

    long getStableOffset();

    long getReadySequence();

    long getReadyOffset();

    @Nullable
    String getWriteFailure();

    int getUnstablePosition();

    int getUnstableRemaining();

    int getPendingWritesCount();
}
