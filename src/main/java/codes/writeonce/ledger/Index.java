package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;

import static java.util.Objects.requireNonNull;

public class Index {

    private final int blockSize;

    private final long fileBlockCount;

    @Nonnull
    private final FileChannel channel;

    private final int levelCount;

    private final long[] levelOffsets;

    private final ByteBuffer[] levelBuffers;

    private final long[] levelBufferPosition;

    private final boolean[] levelBufferDirty;

    private final int blockItemCount;

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

        requireNonNull(channel);

        this.blockSize = blockSize;
        this.fileBlockCount = fileBlockCount;
        this.channel = channel;

        blockItemCount = blockSize / 16;
        int levelCount = 0;
        final var levelOffsets = new ArrayList<Long>();

        var levelBlockCount = fileBlockCount;
        do {
            levelBlockCount = (levelBlockCount + blockItemCount - 1) / blockItemCount;
            for (int i = 0; i < levelCount; i++) {
                levelOffsets.set(i, levelOffsets.get(i) + levelBlockCount);
            }
            levelOffsets.add(0, 0L);
            levelCount++;
        } while (levelBlockCount > 1);

        this.levelCount = levelCount;
        this.levelOffsets = levelOffsets.stream().mapToLong(e -> e).toArray();
        this.levelBuffers = new ByteBuffer[levelCount];
        for (int i = 0; i < levelCount; i++) {
            levelBuffers[i] = ByteBuffer.allocate(blockSize);
        }
        this.levelBufferPosition = new long[levelCount];
        this.levelBufferDirty = new boolean[levelCount];
        Arrays.fill(this.levelBufferPosition, -1);
        Arrays.fill(this.levelBufferDirty, false);

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

        final var byteBuffer = get(0, 0);
        firstSequence = byteBuffer.getLong(0);
        firstOffset = byteBuffer.getLong(8);
    }

    private long findMin() throws IOException {
        return findMinMax(1);
    }

    private long findMax() throws IOException {
        return findMinMax(-1);
    }

    private long findMinMax(int sign) throws IOException {

        long position = 0;
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

    private long findMinMax2(int sign, int level, long position) throws IOException {

        final var byteBuffer = get(level, position);
        int i = 0;
        while (i < blockItemCount) {
            long minMaxSequence = byteBuffer.getLong(i * 16);
            if (minMaxSequence != 0) {
                long item = i;
                long minMaxoffset = byteBuffer.getLong(i * 16 + 8);
                i++;
                while (i < blockItemCount) {
                    final var s = byteBuffer.getLong(i * 16);
                    final var o = byteBuffer.getLong(i * 16 + 8);
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

    public void put(long blockNumber, long sequence, long offset) throws IOException {

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
            final var byteBuffer = get(level, n);
            byteBuffer.putLong(index * 16, sequence);
            byteBuffer.putLong(index * 16 + 8, offset);
            levelBufferDirty[level] = true;
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

    public long findBlock(long sequence, long offset) throws IOException {

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
            final var byteBuffer = get(level, position);
            int middleItem;
            while (true) {
                middleItem = (int) ((fromItem + toItem + 1) / 2);
                final int sign = compare(byteBuffer, middleItem, sequence, offset);
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

    private int compare(@Nonnull ByteBuffer byteBuffer, int position, long sequence, long offset) {

        final var s = byteBuffer.getLong(position * 16);
        if (s < sequence) {
            return -1;
        } else if (s == sequence) {
            final var o = byteBuffer.getLong(position * 16 + 8);
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

    @Nonnull
    private ByteBuffer get(int level, long position) throws IOException {

        final var levelByteBuffer = levelBuffers[level];
        levelByteBuffer.clear();
        final var currentLevelBufferPosition = levelBufferPosition[level];
        if (currentLevelBufferPosition != position) {
            final var levelOffset = levelOffsets[level];
            if (levelBufferDirty[level]) {
                channel.write(levelByteBuffer, (levelOffset + currentLevelBufferPosition) * blockSize);
                levelByteBuffer.clear();
                levelBufferDirty[level] = false;
            }
            channel.read(levelByteBuffer, (levelOffset + position) * blockSize);
            levelBufferPosition[level] = position;
        }
        return levelByteBuffer;
    }

    public void flush() throws IOException {

        for (int level = 0; level < levelCount; level++) {
            if (levelBufferDirty[level]) {
                final var levelByteBuffer = levelBuffers[level];
                levelByteBuffer.clear();
                channel.write(levelByteBuffer, (levelOffsets[level] + levelBufferPosition[level]) * blockSize);
                levelBufferDirty[level] = false;
            }
        }
    }
}
