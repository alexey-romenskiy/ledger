package codes.writeonce.ledger;

import javax.annotation.Nonnull;

public class QueueItem {

    public BlockBuffer buffer;

    public long fileBlockPosition = -1;

    public boolean writing;

    public int pendingSize;

    public int writingSize;

    public int stableSize;

    public long pendingMessageSequence;

    public long pendingMessageOffset;

    public boolean pendingMessageFinished;

    public long writingMessageSequence;

    public long writingMessageOffset;

    public boolean writingMessageFinished;

    public long stableMessageSequence;

    public long stableMessageOffset;

    public boolean stableMessageFinished;

    public QueueItem prevWriting;

    public QueueItem nextWriting;

    public QueueItem(@Nonnull BlockBuffer buffer) {
        this.buffer = buffer;
    }
}
