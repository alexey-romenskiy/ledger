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

    private static final long TOPIC_ID = 1;

    private static final int BLOCK_SIZE = 0x1000;

    private static final long FILE_BLOCK_COUNT = 3;

    @Test
    public void findBlock1() throws IOException {

        final var file = File.createTempFile("ledger-", null);
        try (var index = new Index(TOPIC_ID, BLOCK_SIZE, FILE_BLOCK_COUNT,
                FileChannel.open(file.toPath(), READ, WRITE, CREATE, DELETE_ON_CLOSE))) {
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
        }
    }

    @Test
    public void findBlock1c() throws IOException {

        final var file = File.createTempFile("ledger-", null);
        try (var index = new Index(
                TOPIC_ID, 64, 32, FileChannel.open(file.toPath(), READ, WRITE, CREATE, DELETE_ON_CLOSE))) {
            for (int i = 0; i < 32; i++) {
                index.put(i % 32, 1, i * 100);
            }
            for (int i = 0; i < 32; i++) {
                assertEquals(i % 32, index.findBlock(1, i * 100));
                assertEquals(i % 32, index.findBlock(1, i * 100 + 99));
            }
            for (int i = 32; i < 32 + 3; i++) {
                index.put(i % 32, 1, i * 100);
            }
            for (int i = 3; i < 32 + 3; i++) {
                assertEquals(i % 32, index.findBlock(1, i * 100));
                assertEquals(i % 32, index.findBlock(1, i * 100 + 99));
            }
            for (int i = 32 + 3; i < 32 + 7; i++) {
                index.put(i % 32, 1, i * 100);
            }
            for (int i = 7; i < 32 + 7; i++) {
                assertEquals(i % 32, index.findBlock(1, i * 100));
                assertEquals(i % 32, index.findBlock(1, i * 100 + 99));
            }
            for (int i = 32 + 7; i < 32 + 11; i++) {
                index.put(i % 32, 1, i * 100);
            }
            for (int i = 11; i < 32 + 11; i++) {
                assertEquals(i % 32, index.findBlock(1, i * 100));
                assertEquals(i % 32, index.findBlock(1, i * 100 + 99));
            }
            for (int i = 32 + 11; i < 32 + 15; i++) {
                index.put(i % 32, 1, i * 100);
            }
            for (int i = 15; i < 32 + 15; i++) {
                assertEquals(i % 32, index.findBlock(1, i * 100));
                assertEquals(i % 32, index.findBlock(1, i * 100 + 99));
            }
            for (int i = 32 + 15; i < 32 + 19; i++) {
                index.put(i % 32, 1, i * 100);
            }
            for (int i = 19; i < 32 + 19; i++) {
                assertEquals(i % 32, index.findBlock(1, i * 100));
                assertEquals(i % 32, index.findBlock(1, i * 100 + 99));
            }
            for (int i = 32 + 19; i < 32 + 23; i++) {
                index.put(i % 32, 1, i * 100);
            }
            for (int i = 23; i < 32 + 23; i++) {
                assertEquals(i % 32, index.findBlock(1, i * 100));
                assertEquals(i % 32, index.findBlock(1, i * 100 + 99));
            }
            for (int i = 32 + 23; i < 32 + 27; i++) {
                index.put(i % 32, 1, i * 100);
            }
            for (int i = 27; i < 32 + 27; i++) {
                assertEquals(i % 32, index.findBlock(1, i * 100));
                assertEquals(i % 32, index.findBlock(1, i * 100 + 99));
            }
            for (int i = 32 + 27; i < 32 + 31; i++) {
                index.put(i % 32, 1, i * 100);
            }
            for (int i = 31; i < 32 + 31; i++) {
                assertEquals(i % 32, index.findBlock(1, i * 100));
                assertEquals(i % 32, index.findBlock(1, i * 100 + 99));
            }
        }
    }

    @Test
    public void findBlock1b() throws IOException {

        final var file = File.createTempFile("ledger-", null);
        try (var index = new Index(TOPIC_ID, BLOCK_SIZE, FILE_BLOCK_COUNT,
                FileChannel.open(file.toPath(), READ, WRITE, CREATE))) {
            index.put(0, 1, 0);
            index.put(1, 1, 100);
            index.put(2, 1, 200);
        }
        try (var index = new Index(TOPIC_ID, BLOCK_SIZE, FILE_BLOCK_COUNT,
                FileChannel.open(file.toPath(), READ, WRITE, CREATE, DELETE_ON_CLOSE))) {
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
        }
    }

    @Test
    public void findBlock2a1() throws IOException {

        final var file = File.createTempFile("ledger-", null);
        try (var index = new Index(TOPIC_ID, BLOCK_SIZE, FILE_BLOCK_COUNT,
                FileChannel.open(file.toPath(), READ, WRITE, CREATE, DELETE_ON_CLOSE))) {
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
        }
    }

    @Test
    public void findBlock2a2() throws IOException {

        final var file = File.createTempFile("ledger-", null);
        try (var index = new Index(TOPIC_ID, BLOCK_SIZE, FILE_BLOCK_COUNT,
                FileChannel.open(file.toPath(), READ, WRITE, CREATE, DELETE_ON_CLOSE))) {
            index.put(0, 1, 0);
            index.put(1, 1, 100);
            index.put(2, 1, 200);
            index.remove(0);
            index.remove(0);
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
            assertEquals(2, index.findBlock(1, 300));
            assertEquals(2, index.findBlock(1, 301));
            assertEquals(2, index.findBlock(1, 350));
        }
    }

    @Test
    public void findBlock2b1() throws IOException {

        final var file = File.createTempFile("ledger-", null);
        try (var index = new Index(TOPIC_ID, BLOCK_SIZE, FILE_BLOCK_COUNT,
                FileChannel.open(file.toPath(), READ, WRITE, CREATE))) {
            index.put(0, 1, 0);
            index.put(1, 1, 100);
            index.put(2, 1, 200);
            index.put(0, 1, 300);
        }
        try (var index = new Index(TOPIC_ID, BLOCK_SIZE, FILE_BLOCK_COUNT,
                FileChannel.open(file.toPath(), READ, WRITE, CREATE, DELETE_ON_CLOSE))) {
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
        }
    }

    @Test
    public void findBlock2b2() throws IOException {

        final var file = File.createTempFile("ledger-", null);
        try (var index = new Index(TOPIC_ID, BLOCK_SIZE, FILE_BLOCK_COUNT,
                FileChannel.open(file.toPath(), READ, WRITE, CREATE))) {
            index.put(0, 1, 0);
            index.put(1, 1, 100);
            index.put(2, 1, 200);
            index.remove(0);
            index.remove(0);
        }
        try (var index = new Index(TOPIC_ID, BLOCK_SIZE, FILE_BLOCK_COUNT,
                FileChannel.open(file.toPath(), READ, WRITE, CREATE, DELETE_ON_CLOSE))) {
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
            assertEquals(2, index.findBlock(1, 300));
            assertEquals(2, index.findBlock(1, 301));
            assertEquals(2, index.findBlock(1, 350));
        }
    }

    @Test
    public void getSize() {
        assertEquals(2 * BLOCK_SIZE, Index.getSize(BLOCK_SIZE, FILE_BLOCK_COUNT));
        assertEquals(2 * BLOCK_SIZE, Index.getSize(BLOCK_SIZE, BLOCK_SIZE / 16));
        assertEquals(4 * BLOCK_SIZE, Index.getSize(BLOCK_SIZE, BLOCK_SIZE / 16 + 1));
        assertEquals(4 * BLOCK_SIZE, Index.getSize(BLOCK_SIZE, BLOCK_SIZE * 2 / 16 - 1));
        assertEquals(4 * BLOCK_SIZE, Index.getSize(BLOCK_SIZE, BLOCK_SIZE * 2 / 16));
        assertEquals(5 * BLOCK_SIZE, Index.getSize(BLOCK_SIZE, BLOCK_SIZE * 2 / 16 + 1));
    }
}
