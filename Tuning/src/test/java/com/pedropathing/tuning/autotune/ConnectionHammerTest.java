package com.pedropathing.tuning.autotune;

import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * T6: concurrency hammer. Several client threads race connect / init / die /
 * disconnect while sessions stream; scaled timings. Asserts no deadlock
 * (global test timeout), the single-claim invariant, and no leaked
 * Pedro-Tuning threads at the end.
 */
public class ConnectionHammerTest extends ConnectionTestBase {

    /** Procedure that keeps requesting confirmations until aborted. */
    static final class LoopingProcedure extends Procedure {
        volatile Thread thread;

        LoopingProcedure(String name) {
            super(name, "hammer procedure");
        }

        @Override
        public void run() throws InterruptedException {
            thread = Thread.currentThread();
            while (true) {
                confirmation("Step", "confirm to continue");
            }
        }
    }

    @Test(timeout = 180_000)
    public void hammerNeverDeadlocksOrDoubleClaims() throws Exception {
        startServer(100, 2, 120);

        LoopingProcedure procedure = new LoopingProcedure("Hammer");
        String id = register(procedure);

        int iterationsPerWorker = 40;
        int workers = 5;
        CountDownLatch done = new CountDownLatch(workers);
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicInteger failures = new AtomicInteger();
        Random random = new Random(12345);
        List<Closeable> open = java.util.Collections.synchronizedList(new ArrayList<>());

        for (int w = 0; w < workers; w++) {
            final int seed = w;
            Thread worker = new Thread(() -> {
                Random rng = new Random(seed * 7919);
                try {
                    for (int i = 0; i < iterationsPerWorker && !stop.get(); i++) {
                        int mode = rng.nextInt(4);
                        switch (mode) {
                            case 0: { // well-behaved client: connect, init, confirm a few, leave
                                TestClient client = new TestClient(port);
                                try {
                                    client.connectOrThrow(5_000);
                                    if (rng.nextBoolean()) {
                                        init(client, id);
                                        client.awaitMessageContaining("\"type\":\"", 1_000);
                                        for (int c = 0; c < 3 && !stop.get(); c++) {
                                            String message = client.awaitMessageContaining(
                                                    "\"type\":\"confirm\"", 500);
                                            if (message == null) break;
                                            confirm(client, requestIdOf(message));
                                        }
                                    }
                                } catch (Exception race) {
                                    // Rejections/closed channels are expected outcomes.
                                } finally {
                                    quietClose(client);
                                }
                                break;
                            }
                            case 1: { // silent client: connect, maybe init, never read again
                                RawClient raw = null;
                                try {
                                    raw = new RawClient(port);
                                    open.add(raw);
                                    if (rng.nextBoolean()) init(raw, id);
                                    // Leak it on purpose: the watchdog reaps it.
                                } catch (IOException race) {
                                    // connect storms can be refused; that is fine
                                    if (raw != null) quietClose(raw);
                                }
                                break;
                            }
                            case 2: { // abrupt death
                                try {
                                    RawClient raw = new RawClient(port);
                                    init(raw, id);
                                    raw.rstClose();
                                } catch (IOException race) {
                                    // expected under churn
                                }
                                break;
                            }
                            default: { // instant connect/close churn
                                TestClient client = new TestClient(port);
                                try {
                                    client.connectOrThrow(5_000);
                                } catch (Exception race) {
                                    // fine
                                } finally {
                                    quietClose(client);
                                }
                                break;
                            }
                        }
                        // Single-claim invariant: the static can only ever hold
                        // one socket or none; read it so a torn/impossible state
                        // would surface as an exception here.
                        WebSocketServer.Socket current = WebSocketServer.activeSocket();
                        if (current != null && !server.sockets.contains(current)) {
                            failures.incrementAndGet();
                        }
                        Thread.sleep(rng.nextInt(30));
                    }
                } catch (Exception fatal) {
                    failures.incrementAndGet();
                } finally {
                    done.countDown();
                }
            }, "hammer-" + w);
            worker.start();
        }

        // Global no-deadlock bound on the workers themselves.
        assertTrue("hammer deadlocked", done.await(120, TimeUnit.SECONDS));
        stop.set(true);

        // Reclaim everything: close leaked raw clients, stop the server so no
        // late client message can spawn a session, then let the watchdog kill
        // whatever silent socket still holds the claim, and assert cleanliness.
        for (Closeable closeable : new ArrayList<>(open)) {
            quietClose(closeable);
        }
        server.stop();
        assertTrue("claim never released after hammer",
                awaitTrue(10_000, () -> WebSocketServer.activeSocket() == null));
        assertEquals("unexpected failures in hammer workers", 0, failures.get());
        assertNoNamedThreads("Pedro-Tuning", 10_000);
        assertNull(WebSocketServer.activeSocket());
    }

    private static void quietClose(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception ignored) {
        }
    }
}
