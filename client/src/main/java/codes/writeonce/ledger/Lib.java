package codes.writeonce.ledger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;

import static java.util.Objects.requireNonNull;

public class Lib {

    static {
        try (var in = Lib.class.getClassLoader().getResourceAsStream("META-INF/native/libpthread_java.so")) {
            requireNonNull(in);
            final var path = Files.createTempFile("libpthread_java", ".so");
            try {
                System.out.println("Created tmp file: " + path);
                try (var out = Files.newOutputStream(path)) {
                    in.transferTo(out);
                }
                System.load(path.toString());
            } finally {
                Files.delete(path);
                System.out.println("Deleted tmp file: " + path);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static native int pthread_mutex_lock(long mutexAddr);

    static native int pthread_mutex_unlock(long mutexAddr);

    static native int pthread_cond_wait(long condAddr, long mutexAddr);

    static native int pthread_cond_signal(long condAddr);
}
