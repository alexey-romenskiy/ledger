package codes.writeonce.ledger;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.Assert.assertEquals;

public class IndexTest {

    private static final int BLOCK_SIZE = 0x1000;

    private static final long FILE_BLOCK_COUNT = 3;

    @Test
    public void findBlock1() throws IOException {

        final var file = File.createTempFile("ledger-", null);
        try (var channel = FileChannel.open(file.toPath(), READ, WRITE, CREATE, DELETE_ON_CLOSE)) {
            channel.truncate(Index.getSize(BLOCK_SIZE, FILE_BLOCK_COUNT));
            final var index = new Index(BLOCK_SIZE, FILE_BLOCK_COUNT, channel);
            index.put(0, 1, 0);
            index.put(1, 1, 100);
            index.put(2, 1, 200);
            assertEquals(0, index.findBlock(1, 0));
            assertEquals(0, index.findBlock(1, 1));
            assertEquals(0, index.findBlock(1, 50));
            assertEquals(0, index.findBlock(1, 99));
            assertEquals(1, index.findBlock(1, 100));
            assertEquals(1, index.findBlock(1, 101));
            assertEquals(1, index.findBlock(1, 150));
            assertEquals(1, index.findBlock(1, 199));
            assertEquals(2, index.findBlock(1, 200));
            assertEquals(2, index.findBlock(1, 201));
            assertEquals(2, index.findBlock(1, 250));
            index.flush();
        }
    }

    @Test
    public void findBlock1b() throws IOException {

        final var file = File.createTempFile("ledger-", null);
        try (var channel = FileChannel.open(file.toPath(), READ, WRITE, CREATE)) {
            channel.truncate(Index.getSize(BLOCK_SIZE, FILE_BLOCK_COUNT));
            final var index = new Index(BLOCK_SIZE, FILE_BLOCK_COUNT, channel);
            index.put(0, 1, 0);
            index.put(1, 1, 100);
            index.put(2, 1, 200);
            index.flush();
        }
        try (var channel = FileChannel.open(file.toPath(), READ, WRITE, CREATE, DELETE_ON_CLOSE)) {
            channel.truncate(Index.getSize(BLOCK_SIZE, FILE_BLOCK_COUNT));
            final var index = new Index(BLOCK_SIZE, FILE_BLOCK_COUNT, channel);
            assertEquals(0, index.findBlock(1, 0));
            assertEquals(0, index.findBlock(1, 1));
            assertEquals(0, index.findBlock(1, 50));
            assertEquals(0, index.findBlock(1, 99));
            assertEquals(1, index.findBlock(1, 100));
            assertEquals(1, index.findBlock(1, 101));
            assertEquals(1, index.findBlock(1, 150));
            assertEquals(1, index.findBlock(1, 199));
            assertEquals(2, index.findBlock(1, 200));
            assertEquals(2, index.findBlock(1, 201));
            assertEquals(2, index.findBlock(1, 250));
            index.flush();
        }
    }

    @Test
    public void findBlock2() throws IOException {

        final var file = File.createTempFile("ledger-", null);
        try (var channel = FileChannel.open(file.toPath(), READ, WRITE, CREATE, DELETE_ON_CLOSE)) {
            channel.truncate(Index.getSize(BLOCK_SIZE, FILE_BLOCK_COUNT));
            final var index = new Index(BLOCK_SIZE, FILE_BLOCK_COUNT, channel);
            index.put(0, 1, 0);
            index.put(1, 1, 100);
            index.put(2, 1, 200);
            index.put(0, 1, 300);
            assertEquals(-1, index.findBlock(1, 0));
            assertEquals(-1, index.findBlock(1, 1));
            assertEquals(-1, index.findBlock(1, 50));
            assertEquals(-1, index.findBlock(1, 99));
            assertEquals(1, index.findBlock(1, 100));
            assertEquals(1, index.findBlock(1, 101));
            assertEquals(1, index.findBlock(1, 150));
            assertEquals(1, index.findBlock(1, 199));
            assertEquals(2, index.findBlock(1, 200));
            assertEquals(2, index.findBlock(1, 201));
            assertEquals(2, index.findBlock(1, 250));
            assertEquals(2, index.findBlock(1, 299));
            assertEquals(0, index.findBlock(1, 300));
            assertEquals(0, index.findBlock(1, 301));
            assertEquals(0, index.findBlock(1, 350));
            index.flush();
        }
    }

    @Test
    public void findBlock2b() throws IOException {

        final var file = File.createTempFile("ledger-", null);
        try (var channel = FileChannel.open(file.toPath(), READ, WRITE, CREATE)) {
            channel.truncate(Index.getSize(BLOCK_SIZE, FILE_BLOCK_COUNT));
            final var index = new Index(BLOCK_SIZE, FILE_BLOCK_COUNT, channel);
            index.put(0, 1, 0);
            index.put(1, 1, 100);
            index.put(2, 1, 200);
            index.put(0, 1, 300);
            index.flush();
        }
        try (var channel = FileChannel.open(file.toPath(), READ, WRITE, CREATE, DELETE_ON_CLOSE)) {
            channel.truncate(Index.getSize(BLOCK_SIZE, FILE_BLOCK_COUNT));
            final var index = new Index(BLOCK_SIZE, FILE_BLOCK_COUNT, channel);
            assertEquals(-1, index.findBlock(1, 0));
            assertEquals(-1, index.findBlock(1, 1));
            assertEquals(-1, index.findBlock(1, 50));
            assertEquals(-1, index.findBlock(1, 99));
            assertEquals(1, index.findBlock(1, 100));
            assertEquals(1, index.findBlock(1, 101));
            assertEquals(1, index.findBlock(1, 150));
            assertEquals(1, index.findBlock(1, 199));
            assertEquals(2, index.findBlock(1, 200));
            assertEquals(2, index.findBlock(1, 201));
            assertEquals(2, index.findBlock(1, 250));
            assertEquals(2, index.findBlock(1, 299));
            assertEquals(0, index.findBlock(1, 300));
            assertEquals(0, index.findBlock(1, 301));
            assertEquals(0, index.findBlock(1, 350));
            index.flush();
        }
    }

    @Test
    public void getSize() {
        assertEquals(BLOCK_SIZE, Index.getSize(BLOCK_SIZE, FILE_BLOCK_COUNT));
        assertEquals(BLOCK_SIZE, Index.getSize(BLOCK_SIZE, BLOCK_SIZE / 16));
        assertEquals(3 * BLOCK_SIZE, Index.getSize(BLOCK_SIZE, BLOCK_SIZE / 16 + 1));
        assertEquals(3 * BLOCK_SIZE, Index.getSize(BLOCK_SIZE, BLOCK_SIZE * 2 / 16 - 1));
        assertEquals(3 * BLOCK_SIZE, Index.getSize(BLOCK_SIZE, BLOCK_SIZE * 2 / 16));
        assertEquals(4 * BLOCK_SIZE, Index.getSize(BLOCK_SIZE, BLOCK_SIZE * 2 / 16 + 1));
    }
}
