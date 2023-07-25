package codes.writeonce.ledger;

import jdk.internal.misc.Unsafe;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.time.Instant;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class Main {

    private static final Unsafe UNSAFE = Unsafe.getUnsafe();

    public static void main(String[] args) throws IOException, InterruptedException {

        try (var arena = Arena.openConfined();
             var channel = FileChannel.open(Path.of("/run/devtest/map.txt"), READ, WRITE)) {

            final var segment = channel.map(READ_WRITE, 0, 96, arena.scope());
            final var mutexAddr = segment.address();

            final var condAddress = mutexAddr + 40;
            final var flagAddress = mutexAddr + 88;
            System.out.println(Instant.now() + " locking");
            Lib.pthread_mutex_lock(mutexAddr);
            System.out.println(Instant.now() + " locked");

            while (true) {
                final var x = UNSAFE.getLong(flagAddress) + 1;
                UNSAFE.putLong(flagAddress, x);
                System.out.println("x = " + x);
                Thread.sleep(1000);
//                Lib.pthread_cond_signal(condAddress);
//                Lib.pthread_cond_wait(condAddress, mutexAddr);
            }
//            System.out.println(Instant.now() + " unlocking");
//            Lib.pthread_mutex_unlock(mutexAddr);
//            System.out.println(Instant.now() + " unlocked");
        }
    }
}
