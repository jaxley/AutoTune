package com.pedropathing.tuning.autotune;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * T2: a silently dead client (never reads, never pongs) is reclaimed by the
 * ping watchdog; an RST-aborted client is reclaimed via the exceptional ping
 * future; a healthy client is never killed. Also asserts the watchdog
 * shutdown path terminates its threads.
 */
public class WatchdogReclamationTest extends ConnectionTestBase {

    @Test(timeout = 30_000)
    public void silentDeathIsReclaimedByWatchdog() throws Exception {
        startServer(150, 2, 300);

        BlockingProcedure procedure = new BlockingProcedure("Blocking");
        String id = register(procedure);

        RawClient dead = new RawClient(port);
        try {
            init(dead, id);
            assertTrue("procedure never entered",
                    procedure.entered.await(5_000, TimeUnit.MILLISECONDS));
            assertNotNull(procedure.thread);

            // The client is silent forever: the watchdog must reclaim the
            // claim, abort the session and free the name (kill lands after
            // ~3 ping intervals).
            assertTrue("watchdog never reclaimed the claim",
                    awaitTrue(3_000, () -> WebSocketServer.activeSocket() == null));

            joinThread(procedure.thread, 5_000);

            // The claim is free: a fresh client is accepted immediately.
            TestClient next = new TestClient(port);
            try {
                next.connectOrThrow(5_000);
                init(next, id);
                assertNotNull("client not accepted after reclamation",
                        next.awaitMessageContaining("\"type\":\"init\"", 5_000));
            } finally {
                next.closeOrThrow(2_000);
            }
        } finally {
            dead.close();
        }
    }

    @Test(timeout = 30_000)
    public void rstAbortIsReclaimedByWatchdog() throws Exception {
        startServer(150, 2, 300);

        BlockingProcedure procedure = new BlockingProcedure("Blocking");
        String id = register(procedure);

        RawClient client = new RawClient(port);
        try {
            init(client, id);
            assertTrue("procedure never entered",
                    procedure.entered.await(5_000, TimeUnit.MILLISECONDS));

            client.rstClose();

            assertTrue("watchdog never reclaimed the claim after RST",
                    awaitTrue(3_000, () -> WebSocketServer.activeSocket() == null));
            joinThread(procedure.thread, 5_000);

            TestClient next = new TestClient(port);
            try {
                next.connectOrThrow(5_000);
                init(next, id);
                assertNotNull("client not accepted after reclamation",
                        next.awaitMessageContaining("\"type\":\"init\"", 5_000));
            } finally {
                next.closeOrThrow(2_000);
            }
        } finally {
            client.close();
        }
    }

    /** Healthy client keeps answering pings: the watchdog must never kill it. */
    @Test(timeout = 30_000)
    public void healthyClientIsNeverKilled() throws Exception {
        startServer(100, 2, 300);

        BlockingProcedure procedure = new BlockingProcedure("Blocking");
        String id = register(procedure);

        TestClient healthy = new TestClient(port);
        try {
            healthy.connectOrThrow(5_000);
            init(healthy, id);
            assertNotNull(healthy.awaitMessageContaining("\"type\":\"init\"", 5_000));

            // Park well past several kill windows; the client auto-pongs.
            Thread.sleep(1_200);

            assertEquals(server.sockets.get(0), WebSocketServer.activeSocket());
            assertTrue(healthy.isOpen());
        } finally {
            healthy.closeOrThrow(2_000);
        }
    }

    /** Watchdog stop terminates the scheduler and ping executor threads. */
    @Test(timeout = 15_000)
    public void watchdogShutdownTerminatesItsThreads() throws Exception {
        startServer(150, 2, 300);
        assertNotNull(findThread("Tune-Watchdog"));

        ConnectionWatchdog.stop();
        ConnectionWatchdog.stop(); // idempotent

        assertNoNamedThreads("Tune-Watchdog", 2_000);
        assertNoNamedThreads("Tune-Ping", 2_000);
        assertNull(ConnectionWatchdog.pingExecutor());
    }
}
