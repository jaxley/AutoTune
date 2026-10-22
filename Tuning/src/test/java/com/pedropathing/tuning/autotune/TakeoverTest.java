package com.pedropathing.tuning.autotune;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * T1: a second client is rejected while the first is healthy.
 * T3: a silent (dead but not closed) client is taken over after a probe
 * timeout; a healthy client still rejects the newcomer via the pong path.
 */
public class TakeoverTest extends ConnectionTestBase {

    /** T1: second client rejected while first healthy; first session untouched. */
    @Test
    public void secondClientRejectedWhileFirstHealthy() throws Exception {
        // 1h ping interval: watchdog alive but never ticks within the test; the
        // ping executor still serves takeover probes.
        startServer(3_600_000L, 2, 300);

        BlockingProcedure procedure = new BlockingProcedure("Blocking");
        String id = register(procedure);

        TestClient first = new TestClient(port);
        try {
            first.connectOrThrow(5_000);
            init(first, id);
            assertNotNull(first.awaitMessageContaining("\"type\":\"init\"", 5_000));
            // The session runs and asks for confirmation.
            String confirmation = first.awaitMessageContaining("\"type\":\"confirm\"", 5_000);
            assertNotNull("first client never got the confirmation request", confirmation);
            assertTrue("procedure never entered", procedure.entered.await(5_000, TimeUnit.MILLISECONDS));
            WebSocketServer.Socket firstSocket = server.sockets.get(0);
            assertEquals(firstSocket, WebSocketServer.activeSocket());

            TestClient second = new TestClient(port);
            try {
                second.connectOrThrow(5_000);
                int closeCode = second.awaitClose(3_000);
                assertEquals(1008, closeCode);
                assertEquals("A tuning client is already connected.", second.closeReason);
            } finally {
                second.close();
            }

            // First session untouched: still the claim owner and still open.
            assertEquals(firstSocket, WebSocketServer.activeSocket());
            assertTrue(first.isOpen());
        } finally {
            first.closeOrThrow(2_000);
        }
    }

    /** T3 (dead variant): probe times out, the claim is stolen, the old socket is reclaimed. */
    @Test
    public void takeoverAfterProbeTimeoutOnSilentClient() throws Exception {
        startServer(3_600_000L, 2, 300);

        BlockingProcedure procedure = new BlockingProcedure("Blocking");
        String id = register(procedure);

        RawClient dead = new RawClient(port);
        try {
            init(dead, id);
            assertTrue("procedure never entered", procedure.entered.await(5_000, TimeUnit.MILLISECONDS));
            WebSocketServer.Socket deadSocket = server.sockets.get(0);
            assertEquals(deadSocket, WebSocketServer.activeSocket());
            // Capture by name before the newcomer starts a second session
            // (the shared Procedure instance's field will be overwritten).
            Thread deadSessionThread = findThread("Pedro-Tuning");

            TestClient newcomer = new TestClient(port);
            try {
                newcomer.connectOrThrow(5_000);
                // Newcomer is admitted and can start a fresh session.
                newcomer.initUntilAccepted(id, 5_000);

                assertEquals(server.sockets.get(1), WebSocketServer.activeSocket());

                // The old socket was reclaimed: its reader is unblocked and the
                // server closed its stream.
                assertTrue("old silent socket was never closed by the server",
                        dead.awaitServerClose(5_000));

                // The old Pedro-Tuning thread exited via the abort path.
                joinThread(deadSessionThread, 5_000);
            } finally {
                newcomer.closeOrThrow(2_000);
            }
        } finally {
            dead.close();
        }
    }

    /** T3 (healthy variant): a pong answers the probe, the newcomer is rejected. */
    @Test
    public void healthyClientAnswersProbeAndNewcomerRejected() throws Exception {
        startServer(3_600_000L, 2, 300);

        BlockingProcedure procedure = new BlockingProcedure("Blocking");
        String id = register(procedure);

        TestClient healthy = new TestClient(port);
        try {
            healthy.connectOrThrow(5_000);
            init(healthy, id);
            assertNotNull(healthy.awaitMessageContaining("\"type\":\"init\"", 5_000));
            assertNotNull(healthy.awaitMessageContaining("\"type\":\"confirm\"", 5_000));
            WebSocketServer.Socket healthySocket = server.sockets.get(0);

            TestClient newcomer = new TestClient(port);
            try {
                newcomer.connectOrThrow(5_000);
                int closeCode = newcomer.awaitClose(3_000);
                assertEquals(1008, closeCode);
                assertEquals("A tuning client is already connected.", newcomer.closeReason);
            } finally {
                newcomer.close();
            }

            // The healthy client kept the claim (pong path rejected the probe).
            assertEquals(healthySocket, WebSocketServer.activeSocket());
            assertTrue(healthy.isOpen());
        } finally {
            healthy.closeOrThrow(2_000);
        }
    }

    /**
     * Defensive re-check path: the probed claim is released mid-probe (RST
     * kills the silent client while the newcomer is still waiting on the probe
     * deadline), so the newcomer's takeover re-read finds a free claim.
     */
    @Test
    public void claimReleasedDuringProbeAdmitsNewcomer() throws Exception {
        startServer(3_600_000L, 2, 300);

        BlockingProcedure procedure = new BlockingProcedure("Blocking");
        String id = register(procedure);

        RawClient silent = new RawClient(port);
        try {
            init(silent, id);
            assertTrue("procedure never entered", procedure.entered.await(5_000, TimeUnit.MILLISECONDS));

            TestClient second = new TestClient(port);
            // RST the silent client mid-probe (deadline is 300ms).
            Thread killer = new Thread(() -> {
                try {
                    Thread.sleep(120);
                    silent.rstClose();
                } catch (Exception ignored) {
                }
            });
            killer.start();
            try {
                second.connectOrThrow(5_000);
                second.initUntilAccepted(id, 5_000);
                WebSocketServer.Socket secondSocket = server.sockets.get(1);
                assertEquals(secondSocket, WebSocketServer.activeSocket());
            } finally {
                second.closeOrThrow(2_000);
            }
        } finally {
            silent.close();
        }
    }
}
