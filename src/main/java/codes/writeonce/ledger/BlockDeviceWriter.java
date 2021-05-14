package codes.writeonce.ledger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;

import static com.sun.nio.file.ExtendedOpenOption.DIRECT;
import static java.nio.file.StandardOpenOption.DSYNC;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class BlockDeviceWriter implements BlockWriter, AutoCloseable {

    private static final BlockBuffer POISON = new BlockBuffer(ByteBuffer.allocate(1));

    private static final int BLOCK_SIZE = 0x1000;

    private static final int MAX_CONCURRENT_BLOCKS = 4;

    private static final int BUFFERS_POOL_SIZE = 4;

    private static final int MAX_BEHIND = MAX_CONCURRENT_BLOCKS;

    private final int blockSize;

    private final int maxConcurrentBlocks;

    private final int maxBehind;

    private final ArrayBlockingQueue<BlockBuffer> freeBuffers;

    private long nextWriteBlockNumber;

    private long nextWriteBlockOffset;

    private final TreeMap<Long, Operation> pendingOperations = new TreeMap<>();

    private final TreeMap<Long, Operation> runningOperations = new TreeMap<>();

    private final byte[] zeroedBlock;

    @Nonnull
    private final PersisterListener persisterListener;

    @Nonnull
    private final AsynchronousFileChannel channel;

    private final long fileBlockLength;

    private long freeBlockCount;

    private long nextReadBlockOffset;

    private long readableBlockCount;

    private boolean lastBlockPartial;

    private int lastPartialBlockOffset;

    private boolean failed;

    private final CompletionHandler<Integer, Operation> handler = new CompletionHandler<>() {
        @Override
        public void completed(Integer result, Operation attachment) {

            if (result == blockSize) {
                try {
                    complete(attachment);
                } catch (Error e) {
                    fail(e);
                    throw e;
                } catch (InterruptedException e) {
                    fail(e);
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    fail(e);
                }
            } else {
                fail(new IllegalStateException("Result is " + result));
            }
        }

        @Override
        public void failed(Throwable throwable, Operation attachment) {
            fail(throwable);
        }
    };

    public static void main(String[] args) throws InterruptedException, IOException {

        final var channel = AsynchronousFileChannel.open(Path.of(args[0]), DSYNC, READ, WRITE, DIRECT);
        new BlockDeviceWriter(BLOCK_SIZE, MAX_CONCURRENT_BLOCKS, MAX_BEHIND, BUFFERS_POOL_SIZE, channel,
                channel.size() / BLOCK_SIZE, new PersisterListener() {
            @Override
            public void persisted(long sequence, long offset, boolean pending) {
                // TODO:
            }

            @Override
            public void failed() {
                // TODO:
            }
        });
    }

    public BlockDeviceWriter(int blockSize, int maxConcurrentBlocks, int maxBehind, int buffersPoolSize,
            @Nonnull AsynchronousFileChannel channel, long fileBlockLength,
            @Nonnull PersisterListener persisterListener)
            throws InterruptedException {

        this.blockSize = blockSize;
        this.maxConcurrentBlocks = maxConcurrentBlocks;
        this.maxBehind = maxBehind;
        this.channel = channel;
        this.fileBlockLength = fileBlockLength;
        this.freeBlockCount = fileBlockLength;
        this.freeBuffers = new ArrayBlockingQueue<>(buffersPoolSize);

        this.zeroedBlock = new byte[blockSize];
        this.persisterListener = persisterListener;

        final ByteBuffer wholeBuffer = ByteBuffer.allocateDirect(blockSize * (buffersPoolSize + 1) - 1);
        final int addressModulus = wholeBuffer.alignmentOffset(0, blockSize);
        final int alignedPosition = addressModulus > 0 ? blockSize - addressModulus : 0;

        for (int i = 0; i < buffersPoolSize; i++) {
            final ByteBuffer byteBuffer = wholeBuffer.slice(alignedPosition + i * blockSize, blockSize);
            freeBuffers.put(new BlockBuffer(byteBuffer));
        }
    }

    @Override
    public void fullBlock(long sequence, long offset, boolean pending, @Nonnull BlockBuffer blockBuffer, int remaining)
            throws InterruptedException {

        blockBuffer.getReaderByteBuffer().clear();

        final var op1 =
                new Operation(nextWriteBlockNumber, nextWriteBlockOffset, sequence, offset, pending, blockBuffer, true);
        final boolean send;
        final Operation op2;

        synchronized (this) {

            if (failed) {
                throw new IllegalStateException();
            }

            nextWriteBlockNumber++;
            nextWriteBlockOffset = (nextWriteBlockOffset + 1) % fileBlockLength;

            if (lastBlockPartial) {
                lastBlockPartial = false;
            } else {
                freeBlockCount--;
            }

            if (freeBlockCount >= 0 && !runningOperations.containsKey(op1.blockNumber) && canForward(op1.blockNumber)) {
                runningOperations.put(op1.blockNumber, op1);
                send = true;
                op2 = null;
            } else {
                send = false;
                op2 = pendingOperations.put(op1.blockNumber, op1);
                if (op2 == null) {
                    op1.startNanos = System.nanoTime();
                } else {
                    op1.startNanos = op2.startNanos;
                }
            }
        }

        if (send) {
            send(op1);
        }

        if (op2 != null) {
            reclaim(op2.blockBuffer);
        }
    }

    @Override
    public void partialBlock(long sequence, long offset, boolean pending, @Nonnull BlockBuffer partialBuffer, int end)
            throws InterruptedException {

        final var buffer = freeBuffers.take();
        if (buffer == POISON) {
            freeBuffers.put(POISON);
            throw new IllegalStateException();
        }

        buffer.getWriterByteBuffer().put(0, partialBuffer.getReaderByteBuffer(), 0, end);

        final var op1 =
                new Operation(nextWriteBlockNumber, nextWriteBlockOffset, sequence, offset, pending, buffer, false);
        final boolean send;
        final Operation op2;

        synchronized (this) {

            if (failed) {
                throw new IllegalStateException();
            }

            if (!lastBlockPartial) {
                lastBlockPartial = true;
                freeBlockCount--;
            }

            lastPartialBlockOffset = end;

            if (freeBlockCount >= 0 && !runningOperations.containsKey(op1.blockNumber) && canForward(op1.blockNumber)) {
                runningOperations.put(op1.blockNumber, op1);
                send = true;
                op2 = null;
            } else {
                send = false;
                op2 = pendingOperations.put(op1.blockNumber, op1);
                if (op2 == null) {
                    op1.startNanos = System.nanoTime();
                } else {
                    op1.startNanos = op2.startNanos;
                }
            }
        }

        if (send) {
            send(op1);
        }

        if (op2 != null) {
            reclaim(op2.blockBuffer);
        }
    }

    private void reclaim(@Nonnull BlockBuffer blockBuffer) throws InterruptedException {
        blockBuffer.getWriterByteBuffer().clear().put(zeroedBlock).clear();
        freeBuffers.put(blockBuffer);
    }

    @Nullable
    private Operation getNext() {

        if (pendingOperations.isEmpty()) {
            return null;
        }

        final var operation = pendingOperations.remove(pendingOperations.firstKey());
        runningOperations.put(operation.blockNumber, operation);
        return operation;
    }

    private boolean canForward(long nextBlock) {

        if (runningOperations.isEmpty()) {
            return true;
        } else {
            return Integer.min(maxConcurrentBlocks, (int) (runningOperations.firstKey() + maxBehind - nextBlock)) > 0;
        }
    }

    private void send(@Nonnull Operation op) {

        channel.write(op.blockBuffer.getReaderByteBuffer(), op.fileBlockOffset * blockSize, op, handler);
    }

    private void fail(@Nonnull Throwable throwable) {

        final boolean notify;

        synchronized (this) {
            notify = !failed;
            failed = true;
        }

        do {
            freeBuffers.clear();
        } while (!freeBuffers.offer(POISON));

        if (notify) {
            persisterListener.failed();
        }
    }

    private void complete(@Nonnull Operation op1) throws InterruptedException {

        final var ops = new ArrayList<Operation>(1);
        final boolean notify;

        synchronized (this) {

            if (failed) {
                return;
            }

            if (op1.complete) {
                readableBlockCount++;
            }

            var op2 = pendingOperations.remove(op1.blockNumber);
            if (op2 == null) {
                runningOperations.remove(op1.blockNumber);
                final var iterator = pendingOperations.values().iterator();
                while (iterator.hasNext() && freeBlockCount >= 0) {
                    op2 = iterator.next();
                    if (!runningOperations.containsKey(op2.blockNumber)) {
                        if (canForward(op2.blockNumber)) {
                            runningOperations.put(op2.blockNumber, op2);
                            ops.add(op2);
                        } else {
                            break;
                        }
                    }
                }
            } else {
                runningOperations.put(op2.blockNumber, op2);
            }

            notify = runningOperations.lowerKey(op1.blockNumber) == null &&
                     pendingOperations.lowerKey(op1.blockNumber) == null;
        }

        for (final var op : ops) {
            send(op);
        }

        reclaim(op1.blockBuffer);

        if (notify) {
            persisterListener.persisted(op1.sequence, op1.offset, op1.pending);
        }
    }

    @Override
    public void close() throws Exception {
        channel.close();
    }

    private static class Operation {

        private final long blockNumber;

        private final long fileBlockOffset;

        private final long sequence;

        private final long offset;

        private final boolean pending;

        private final BlockBuffer blockBuffer;

        private final boolean complete;

        private long startNanos;

        public Operation(long blockNumber, long fileBlockOffset, long sequence, long offset, boolean pending,
                BlockBuffer blockBuffer, boolean complete) {
            this.blockNumber = blockNumber;
            this.fileBlockOffset = fileBlockOffset;
            this.sequence = sequence;
            this.offset = offset;
            this.pending = pending;
            this.blockBuffer = blockBuffer;
            this.complete = complete;
        }
    }
}
