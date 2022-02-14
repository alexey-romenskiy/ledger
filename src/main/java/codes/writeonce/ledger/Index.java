package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.util.Objects.requireNonNull;

public class Index implements Closeable {

    private final int blockSize;

    private final long fileBlockCount;

    @Nonnull
    private final FileChannel channel;

    private final int levelCount;

    private final int[] levelOffsets;

    private final int blockItemCount;

    private final MappedByteBuffer buffer;

    private long firstFilledBlock;

    private long filledBlockCount;

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

        long total = 0;
        var levelBlockCount = fileBlockCount;
        do {
            levelBlockCount = (levelBlockCount + blockItemCount - 1) / blockItemCount;
            total += levelBlockCount;
        } while (levelBlockCount > 1);

        return total * blockSize;
    }

    public Index(int blockSize, long fileBlockCount, @Nonnull FileChannel channel) throws IOException {

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

        this.blockSize = blockSize;
        this.fileBlockCount = fileBlockCount;
        this.channel = channel;
        channel.truncate(size);
        buffer = channel.map(READ_WRITE, 0, size);

        blockItemCount = blockSize / 16;
        int levelCount = 0;
        final var levelOffsets = new ArrayList<Integer>();

        var levelBlockCount = fileBlockCount;
        do {
            levelBlockCount = (levelBlockCount + blockItemCount - 1) / blockItemCount;
            for (int i = 0; i < levelCount; i++) {
                levelOffsets.set(i, levelOffsets.get(i) + (int) levelBlockCount);
            }
            levelOffsets.add(0, 0);
            levelCount++;
        } while (levelBlockCount > 1);

        this.levelCount = levelCount;
        this.levelOffsets = levelOffsets.stream().mapToInt(e -> e).toArray();

        final var max = findMax();
        final var min = findMin();
        if (max == -1) {
            if (min == -1) {
                firstFilledBlock = 0;
                filledBlockCount = 0;
            } else {
                throw new IllegalArgumentException();
            }
        } else {
            if (min == -1) {
                throw new IllegalArgumentException();
            } else {
                firstFilledBlock = min;
                filledBlockCount = (max < min ? fileBlockCount + max - min : max - min) + 1;
            }
        }

        firstSequence = buffer.getLong(0);
        firstOffset = buffer.getLong(8);
    }

    public long getFirstFilledBlock() {
        return firstFilledBlock;
    }

    public long getFilledBlockCount() {
        return filledBlockCount;
    }

    private int findMin() {
        return findMinMax(1);
    }

    private int findMax() {
        return findMinMax(-1);
    }

    private int findMinMax(int sign) {

        int position = 0;
        for (int level = 0; level < levelCount; level++) {
            final var item = findMinMax2(sign, level, position);
            if (item == -1) {
                if (level == 0) {
                    return -1;
                } else {
                    throw new IllegalArgumentException();
                }
            }
            position = position * blockItemCount + item;
        }
        return position;
    }

    private int findMinMax2(int sign, int level, int position) {

        final var base = (levelOffsets[level] + position) * blockSize;
        int i = 0;
        while (i < blockItemCount) {
            long minMaxSequence = buffer.getLong(base + i * 16);
            if (minMaxSequence != 0) {
                int item = i;
                long minMaxoffset = buffer.getLong(base + i * 16 + 8);
                i++;
                while (i < blockItemCount) {
                    final var s = buffer.getLong(base + i * 16);
                    final var o = buffer.getLong(base + i * 16 + 8);
                    if (s != 0) {
                        final var sign1 = Long.compare(minMaxSequence, s);
                        if (sign1 == 0) {
                            final var sign2 = Long.compare(minMaxoffset, o);
                            if (sign2 == 0) {
                                throw new IllegalArgumentException();
                            } else if (sign2 == sign) {
                                minMaxoffset = o;
                                item = i;
                            }
                        } else if (sign1 == sign) {
                            minMaxSequence = s;
                            minMaxoffset = o;
                            item = i;
                        }
                    }
                    i++;
                }
                return item;
            }
            i++;
        }
        return -1;
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

        int index;
        int level = levelCount;
        long n = blockNumber;

        while (true) {
            level--;
            index = (int) (n % blockItemCount);
            n /= blockItemCount;
            final var base = (levelOffsets[level] + (int) n) * blockSize;
            buffer.putLong(base + index * 16, 0);
            buffer.putLong(base + index * 16 + 8, 0);
            if (index != 0) {
                break;
            }
            if (level == 0) {
                firstSequence = 0;
                firstOffset = 0;
                break;
            }
        }
    }

    /**
     * Записать в индекс, что в данном блоке первым является данный sequence/offset.
     */
    public void put(long blockNumber, long sequence, long offset) {

        if (filledBlockCount == fileBlockCount) {
            if (blockNumber != firstFilledBlock) {
                throw new IllegalArgumentException();
            }
            firstFilledBlock = (firstFilledBlock + 1) % fileBlockCount;
        } else {
            if (filledBlockCount == 0) {
                if (blockNumber != 0) {
                    throw new IllegalArgumentException();
                }
                firstFilledBlock = blockNumber;
            } else if (blockNumber != (firstFilledBlock + filledBlockCount) % fileBlockCount) {
                throw new IllegalArgumentException();
            }
            filledBlockCount++;
        }

        int index;
        int level = levelCount;
        long n = blockNumber;

        while (true) {
            level--;
            index = (int) (n % blockItemCount);
            n /= blockItemCount;
            final var base = (levelOffsets[level] + (int) n) * blockSize;
            buffer.putLong(base + index * 16, sequence);
            buffer.putLong(base + index * 16 + 8, offset);
            if (index != 0) {
                break;
            }
            if (level == 0) {
                firstSequence = sequence;
                firstOffset = offset;
                break;
            }
        }
    }

    public long findBlock(@Nonnull Position position) {
        return findBlock(position.sequence, position.offset);
    }

    /**
     * Найти номер блока, в котором записан ближайший не меньший sequence/offset.
     */
    public long findBlock(long sequence, long offset) {

        if (filledBlockCount == 0) {
            return -1;
        }

        var from = firstFilledBlock;
        var to = (firstFilledBlock + filledBlockCount - 1) % fileBlockCount;

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

        while (true) {
            long position;
            long fromItem = from;
            long toItem = to;
            int i = 1;
            long levelFactor = 1;
            while (true) {
                position = fromItem / blockItemCount;
                if (position == toItem / blockItemCount) {
                    fromItem %= blockItemCount;
                    toItem %= blockItemCount;
                    break;
                }
                levelFactor *= blockItemCount;
                i++;
            }
            final var level = levelCount - i;
            final var base = (levelOffsets[level] + (int) position) * blockSize;
            int middleItem;
            while (true) {
                middleItem = (int) ((fromItem + toItem + 1) / 2);
                final int sign = compare(base, middleItem, sequence, offset);
                if (sign < 0) {
                    from = (position * blockItemCount + middleItem) * levelFactor;
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
                        to = (position * blockItemCount + middleItem) * levelFactor - 1;
                    }
                } else {
                    return (position * blockItemCount + middleItem) * levelFactor;
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
    public void close() throws IOException {
        channel.close();
    }
}
