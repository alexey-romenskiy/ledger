package codes.writeonce.ledger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.util.Objects.requireNonNull;

public class Index implements Closeable {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final long topicId;

    private final int fileBlockCount;

    private final int levelCount;

    private final int[] levelOffsets;

    private final int blockItemCount;

    private final MappedByteBuffer buffer;

    private int firstFilledBlock;

    private int filledBlockCount;

    private long firstSequence;

    private long firstOffset;

    public static long getSize(int blockSize, long fileBlockCount) {

        if (blockSize < 16) {
            throw new IllegalArgumentException();
        }

        if (blockSize % 16 != 0) {
            throw new IllegalArgumentException();
        }

        if (fileBlockCount < 1) {
            throw new IllegalArgumentException();
        }

        final var blockItemCount = blockSize / 16;

        long total = 1;
        var levelBlockCount = fileBlockCount;
        do {
            levelBlockCount = (levelBlockCount + blockItemCount - 1) / blockItemCount;
            total += levelBlockCount;
        } while (levelBlockCount > 1);

        return total * blockSize;
    }

    public Index(long topicId, int blockSize, long fileBlockCount, @Nonnull FileChannel channel) throws IOException {

        if (blockSize < 16) {
            throw new IllegalArgumentException();
        }

        if (blockSize % 16 != 0) {
            throw new IllegalArgumentException();
        }

        if (fileBlockCount < 1) {
            throw new IllegalArgumentException();
        }

        final var size = getSize(blockSize, fileBlockCount);
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException();
        }

        requireNonNull(channel);

        if (fileBlockCount > Integer.MAX_VALUE) {
            throw new IllegalArgumentException();
        }

        this.topicId = topicId;

        this.fileBlockCount = (int) fileBlockCount;
        if (channel.size() < size) {
            channel.write(ByteBuffer.allocate(1), size - 1);
        }
        buffer = channel.map(READ_WRITE, 0, size);
        channel.close();

        blockItemCount = blockSize / 16;
        levelOffsets = getLevelOffsets(fileBlockCount, blockItemCount, blockSize);
        levelCount = levelOffsets.length;

        firstFilledBlock = buffer.getInt(0);
        filledBlockCount = buffer.getInt(4);

        if (firstFilledBlock < 0) {
            throw new IllegalArgumentException();
        }

        if (firstFilledBlock >= fileBlockCount) {
            throw new IllegalArgumentException();
        }

        if (filledBlockCount < 0) {
            throw new IllegalArgumentException();
        }

        if (filledBlockCount > fileBlockCount) {
            throw new IllegalArgumentException();
        }

        firstSequence = buffer.getLong(levelOffsets[0]);
        firstOffset = buffer.getLong(levelOffsets[0] + 8);

        logger.info("LEDGER#{}: Index initialized: firstFilledBlock={} filledBlockCount={}", topicId, firstFilledBlock,
                filledBlockCount);
    }

    static int[] getLevelOffsets(long fileBlockCount, int blockItemCount, int blockSize) {

        final var levelOffsets = new ArrayList<Integer>();

        var levelBlockCount = fileBlockCount;
        do {
            levelBlockCount = (levelBlockCount + blockItemCount - 1) / blockItemCount;
            for (int i = 0; i < levelOffsets.size(); i++) {
                levelOffsets.set(i, levelOffsets.get(i) + (int) levelBlockCount);
            }
            levelOffsets.add(0, 1);
        } while (levelBlockCount > 1);

        return levelOffsets.stream().mapToInt(e -> e * blockSize).toArray();
    }

    public long getFirstFilledBlock() {
        return firstFilledBlock;
    }

    public long getFilledBlockCount() {
        return filledBlockCount;
    }

    /**
     * Убрать блок из индекса.
     */
    public void remove(long blockNumber) {

        if (filledBlockCount == fileBlockCount) {
            if (blockNumber != firstFilledBlock) {
                throw new IllegalArgumentException();
            }
        } else {
            if (filledBlockCount == 0) {
                return;
            } else {
                final var end = (firstFilledBlock + filledBlockCount) % fileBlockCount;
                if (end < firstFilledBlock) {
                    if (blockNumber > firstFilledBlock || blockNumber < end) {
                        throw new IllegalArgumentException();
                    }
                } else {
                    if (blockNumber > firstFilledBlock && blockNumber < end) {
                        throw new IllegalArgumentException();
                    }
                }
                if (blockNumber != firstFilledBlock) {
                    return;
                }
            }
        }
        firstFilledBlock = (firstFilledBlock + 1) % fileBlockCount;
        filledBlockCount--;
        buffer.putInt(0, firstFilledBlock);
        buffer.putInt(4, filledBlockCount);

        int level = levelCount;
        int n = (int) blockNumber;

        while (true) {
            level--;
            final var base = levelOffsets[level];
            buffer.putLong(base + n * 16, 0);
            buffer.putLong(base + n * 16 + 8, 0);
            if (n % blockItemCount != 0) {
                break;
            }
            if (level == 0) {
                firstSequence = 0;
                firstOffset = 0;
                break;
            }
            n /= blockItemCount;
        }
    }

    public void get(long blockNumber, @Nonnull Position position) {

        final var base = levelOffsets[levelCount - 1] + (int) blockNumber * 16;
        position.sequence = buffer.getLong(base);
        position.offset = buffer.getLong(base + 8);
    }

    /**
     * Записать в индекс, что в данном блоке первым является данный sequence/offset.
     */
    public void put(long blockNumber, long sequence, long offset) {

        if (filledBlockCount == fileBlockCount) {
            if (blockNumber != firstFilledBlock) {
                throw new IllegalArgumentException(
                        "Expected " + firstFilledBlock +
                        " (firstFilledBlock=" + firstFilledBlock +
                        " filledBlockCount=" + filledBlockCount +
                        " fileBlockCount=" + fileBlockCount +
                        "), but " + blockNumber + " encountered");
            }
            firstFilledBlock = (firstFilledBlock + 1) % fileBlockCount;
            buffer.putInt(0, firstFilledBlock);
        } else {
            if (filledBlockCount == 0) {
                if (blockNumber != 0) {
                    throw new IllegalArgumentException(
                            "Expected 0 (firstFilledBlock=" + firstFilledBlock +
                            " filledBlockCount=" + filledBlockCount +
                            " fileBlockCount=" + fileBlockCount +
                            "), but " + blockNumber + " encountered");
                }
                firstFilledBlock = (int) blockNumber;
                buffer.putInt(0, firstFilledBlock);
            } else {
                final var expectedBlockNumber = (firstFilledBlock + filledBlockCount) % fileBlockCount;
                if (blockNumber != expectedBlockNumber) {
                    throw new IllegalArgumentException(
                            "Expected " + expectedBlockNumber +
                            " (firstFilledBlock=" + firstFilledBlock +
                            " filledBlockCount=" + filledBlockCount +
                            " fileBlockCount=" + fileBlockCount +
                            "), but " + blockNumber + " encountered");
                }
            }
            filledBlockCount++;
            buffer.putInt(4, filledBlockCount);
        }

        int level = levelCount;
        int n = (int) blockNumber;

        while (true) {
            level--;
            final var base = levelOffsets[level];
            buffer.putLong(base + n * 16, sequence);
            buffer.putLong(base + n * 16 + 8, offset);
            if (n % blockItemCount != 0) {
                break;
            }
            if (level == 0) {
                firstSequence = sequence;
                firstOffset = offset;
                break;
            }
            n /= blockItemCount;
        }
    }

    /**
     * Найти номер блока, в начале которого записан ближайший не превосходящий sequence/offset.
     */
    public long findBlock(long sequence, long offset) {

        if (filledBlockCount == 0) {
            return -1;
        }

        int from = firstFilledBlock;
        int to = (firstFilledBlock + filledBlockCount - 1) % fileBlockCount;

        if (from > to) {
            final var sign = compareFisrt(sequence, offset);
            if (sign == 0) {
                return 0;
            } else if (sign < 0) {
                from = 0;
            } else {
                to = fileBlockCount - 1;
            }
        }

        final var signFrom = compare(levelOffsets[levelCount - 1], from, sequence, offset);
        if (signFrom > 0) {
            return -1;
        } else if (signFrom == 0) {
            return from;
        }

        while (true) {
            int fromItem = from;
            int toItem = to;
            int i = 1;
            int levelFactor = 1;
            while (true) {
                final int fromItem2 = fromItem / blockItemCount;
                final int toItem2 = toItem / blockItemCount;
                if (fromItem2 == toItem2) {
                    break;
                }
                final var r = fromItem % blockItemCount;
                if (r == 0) {
                    fromItem = fromItem2;
                    toItem = toItem2;
                } else {
                    final var n = fromItem - r + blockItemCount;
                    final var sign = compare(levelOffsets[levelCount - i], n, sequence, offset);
                    if (sign < 0) {
                        from = n * levelFactor;
                        if (fromItem2 + 1 == toItem2) {
                            fromItem = n;
                            break;
                        } else {
                            fromItem = fromItem2 + 1;
                            toItem = toItem2;
                        }
                    } else if (sign > 0) {
                        toItem = n - 1;
                        to = n * levelFactor - 1;
                        break;
                    } else {
                        return (long) n * levelFactor;
                    }
                }
                levelFactor *= blockItemCount;
                i++;
            }
            int middleItem;
            while (true) {
                middleItem = (fromItem + toItem + 1) / 2;
                final int sign = compare(levelOffsets[levelCount - i], middleItem, sequence, offset);
                if (sign < 0) {
                    from = middleItem * levelFactor;
                    if (middleItem == toItem) {
                        if (from == to) {
                            return from;
                        }
                        break;
                    } else {
                        fromItem = middleItem;
                    }
                } else if (sign > 0) {
                    if (middleItem == fromItem) {
                        return -1;
                    } else {
                        toItem = middleItem - 1;
                        to = middleItem * levelFactor - 1;
                    }
                } else {
                    return (long) middleItem * levelFactor;
                }
            }
        }
    }

    private int compare(int base, int position, long sequence, long offset) {

        final var s = buffer.getLong(base + position * 16);
        if (s < sequence) {
            return -1;
        } else if (s == sequence) {
            final var o = buffer.getLong(base + position * 16 + 8);
            return Long.compare(o, offset);
        } else {
            return 1;
        }
    }

    private int compareFisrt(long sequence, long offset) {

        if (firstSequence < sequence) {
            return -1;
        } else if (firstSequence == sequence) {
            return Long.compare(firstOffset, offset);
        } else {
            return 1;
        }
    }

    @Override
    public void close() {
        // empty
    }
}
