package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class Slot {

    private static final String SLOT_SUFFIX = ".slot";

    @Nonnull
    private final Ledger ledger;

    final int maxPrefetched;

    private final int blockSize;

    private final long id;

    @Nonnull
    public final Path path;

    @Nonnull
    public final Position position;

    public final Position readPosition;

    final SlotItem[] queue;

    int currentPosition;

    int queueUsed;

    long blockFilePosition;

    Slot prev;

    Slot next;

    public Slot(@Nonnull Ledger ledger, int maxPrefetched, int blockSize, long id, @Nonnull Path path,
            @Nonnull Position position) {
        this.ledger = ledger;
        this.maxPrefetched = maxPrefetched;
        this.blockSize = blockSize;
        this.id = id;
        this.path = path;
        this.position = position;
        this.readPosition = position.copy();
        queue = new SlotItem[maxPrefetched];
    }

    public void consume(long sequence, long offset) throws IOException {

        final var r = position.compareTo(sequence, offset);

        if (r > 0) {
            throw new IllegalArgumentException();
        }

        if (r == 0) {
            return;
        }

        if (readPosition.compareTo(sequence, offset) < 0) {
            throw new IllegalArgumentException();
        }

        Ledger.acquire(ledger.queueLock);
        try {
            if (ledger.readyMessageSequence < sequence ||
                ledger.readyMessageSequence == sequence && ledger.readyMessageOffset < offset) {
                throw new IllegalArgumentException();
            }
            position.sequence = sequence;
            position.offset = offset;
            if (ledger.slotMin == this) {
                try {
                    ledger.adjustMinSlot();
                } catch (LedgerException e) {
                    throw new IllegalStateException("Incorrect slot position", e);// BUG!
                }
            }
        } finally {
            Ledger.release(ledger.queueLock);
        }

        writeSlot();
    }

    public void seek(long sequence, long offset) {

    }

    public void read(@Nonnull StreamReceiver receiver) {

    }

    private void writeSlot() throws IOException {

        final var tmpPath = path.getParent().resolve(path.getFileName().toString() + ".tmp");
        Files.write(tmpPath, Arrays.asList(String.valueOf(position.sequence), String.valueOf(position.offset)), UTF_8);
        Files.move(tmpPath, path, REPLACE_EXISTING, ATOMIC_MOVE);
    }

    @Nullable
    static Slot max(@Nullable Slot slots) {

        if (slots == null) {
            return null;
        }

        var max = slots;
        var item = slots;

        while (true) {
            item = item.next;
            if (item == slots) {
                break;
            }
            if (item.position.compareTo(max.position) > 0) {
                max = item;
            }
        }

        return max;
    }

    @Nullable
    static Slot min(@Nullable Slot slots) {

        if (slots == null) {
            return null;
        }

        var min = slots;
        var item = slots;

        while (true) {
            item = item.next;
            if (item == slots) {
                break;
            }
            if (item.position.compareTo(min.position) < 0) {
                min = item;
            }
        }

        return min;
    }

    @Nullable
    static Slot readSlots(@Nonnull Ledger ledger, int maxPrefetched, int blockSize, @Nonnull Path path)
            throws IOException {

        final var paths = Files.list(path).filter(e -> e.getFileName().toString().endsWith(SLOT_SUFFIX)).toList();
        Slot head = null;
        for (final var p : paths) {
            final var name = p.getFileName().toString();
            final var id = Long.parseLong(name.substring(0, name.length() - SLOT_SUFFIX.length()));
            final var slot = new Slot(ledger, maxPrefetched, blockSize, id, p, readSlot(p));
            if (head == null) {
                head = slot;
                slot.prev = slot;
                slot.next = slot;
            } else {
                final var tail = head.prev;
                slot.prev = tail;
                slot.next = head;
                head.prev = slot;
                tail.next = slot;
            }
        }
        return head;
    }

    @Nonnull
    private static Position readSlot(@Nonnull Path path) throws IOException {

        final var lines = Files.readAllLines(path, UTF_8);
        final var sequence = Long.parseLong(lines.get(0));
        final var offset = Long.parseLong(lines.get(1));
        return new Position(sequence, offset);
    }
}
