package codes.writeonce.ledger;

import java.util.stream.IntStream;

import static java.util.stream.Collectors.joining;

final class TestUtils {

    public static final int BLOCK_SIZE = 48;

    public static String arrayToString(byte[] array) {
        return IntStream.range(0, array.length)
                .map(i -> array[i] & 0xff)
                .mapToObj(n -> n < 32 ? String.format("%02X", n) : String.format("%02X|%c", n, (char) n))
                .collect(joining(", "));
    }

    private TestUtils() {
        // empty
    }
}
