package codes.writeonce.ledger;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;

public class LedgerTest {

    @Test
    public void open() throws IOException {

        final var ledger =
                new Ledger(Path.of("foo.ledger"), 0, 4096, 1, Path.of("foo.index"), 0, 4096, 1, Path.of("foo.pid"));
    }
}
