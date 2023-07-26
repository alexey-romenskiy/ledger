package codes.writeonce.ledger.ssl;

import codes.writeonce.concurrency.HiveTimer;
import codes.writeonce.concurrency.NioHive;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public interface SessionControl extends AutoCloseable {

    void changeId(@Nonnull String id);

    void close() throws ExecutionException, InterruptedException;

    void forcedShutdown();

    @Nonnull
    CompletableFuture<Void> shutdown();

    @Nonnull
    HiveTimer getReaderTimer();

    @Nonnull
    HiveTimer getWriterTimer();

    @Nonnull
    NioHive<String> getReaderHive();

    @Nonnull
    NioHive<String> getWriterHive();

    void outboundReady(@Nonnull ByteBuffer[] buffers, int offset, int length);

    void inboundReady(@Nonnull ByteBuffer[] buffers, int offset, int length);
}
