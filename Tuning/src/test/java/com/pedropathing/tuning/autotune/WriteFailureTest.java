package com.pedropathing.tuning.autotune;

import fi.iki.elonen.NanoHTTPD;
import fi.iki.elonen.NanoWSD;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * T4: a failing write releases the claim and aborts the session through the
 * sendMessage write-failure path. The failure is injected deterministically
 * by a Socket subclass that throws from sendFrame once armed (loopback TCP
 * cannot produce a real half-dead stream: the reader thread would always
 * observe the kill first).
 */
public class WriteFailureTest extends ConnectionTestBase {

    /** Socket whose frame writes fail after the test arms it. */
    static final class FailingSocket extends WebSocketServer.Socket {
        volatile boolean failWrites;

        FailingSocket(NanoHTTPD.IHTTPSession handshake) {
            super(handshake);
        }

        @Override
        public void sendFrame(NanoWSD.WebSocketFrame frame) throws IOException {
            if (failWrites) {
                throw new IOException("synthetic write failure");
            }
            super.sendFrame(frame);
        }
    }

    @Test(timeout = 30_000)
    public void writeFailureReleasesClaimAndAbortsSession() throws Exception {
        // Huge ping interval: no watchdog pings, so the only failure source is
        // the injected one and no reader-thread kill can race us (the stream
        // itself is healthy).
        startServer(3_600_000L, 2, 300,
                handshake -> new FailingSocket(handshake));

        BlockingProcedure procedure = new BlockingProcedure("Blocking");
        String id = register(procedure);

        TestClient client = new TestClient(port);
        try {
            client.connectOrThrow(5_000);
            init(client, id);
            assertNotNull(client.awaitMessageContaining("\"type\":\"init\"", 5_000));
            String confirmation = client.awaitMessageContaining("\"type\":\"confirm\"", 5_000);
            assertNotNull(confirmation);
            assertTrue("procedure never entered", procedure.entered.await(5_000, TimeUnit.MILLISECONDS));

            FailingSocket socket = (FailingSocket) server.sockets.get(0);
            assertEquals(socket, WebSocketServer.activeSocket());

            // Arm the fault, then confirm: the procedure's next outbound frame
            // (the second confirmation request) must hit the write-failure
            // path inside sendMessage.
            socket.failWrites = true;
            confirm(client, requestIdOf(confirmation));

            assertTrue("write failure did not release the claim",
                    awaitTrue(5_000, () -> WebSocketServer.activeSocket() == null));
            joinThread(procedure.thread, 5_000);

            // The claim is free again: a new client is accepted.
            TestClient next = new TestClient(port);
            try {
                next.connectOrThrow(5_000);
                init(next, id);
                assertNotNull("client not accepted after write-failure reclamation",
                        next.awaitMessageContaining("\"type\":\"init\"", 5_000));
            } finally {
                next.closeOrThrow(2_000);
            }
        } finally {
            client.closeOrThrow(2_000);
        }
    }
}
