package com.pedropathing.tuning.autotune;

import org.firstinspires.ftc.robotcore.internal.opmode.OpModeMeta;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * T5: a takeover while a tuning op mode is active stops the op mode exactly
 * once (through the Hooks.OpModeRunner seam, which stands in for the FTC
 * OpModeManager/Sinister calls on the JVM) and lets the newcomer start a
 * fresh session.
 */
public class TakeoverDuringOpModeTest extends ConnectionTestBase {

    @Test(timeout = 30_000)
    public void takeoverStopsActiveOpModeExactlyOnce() throws Exception {
        AtomicInteger startCount = new AtomicInteger();
        AtomicInteger stopCount = new AtomicInteger();
        Hooks.setOpModeRunner(new Hooks.OpModeRunner() {
            @Override
            public void register(OpModeMeta meta, TuningOpMode<?> opMode) {
            }

            @Override
            public void init(OpModeMeta meta) {
            }

            @Override
            public void startActiveOpMode() {
                startCount.incrementAndGet();
            }

            @Override
            public void requestOpModeStop(TuningOpMode<?> opMode) {
                stopCount.incrementAndGet();
            }

            @Override
            public void unregister(OpModeMeta meta) {
            }
        });

        startServer(3_600_000L, 2, 300);

        OpModeProcedure procedure = new OpModeProcedure("OpModeBlocking");
        String id = register(procedure);

        RawClient dead = new RawClient(port);
        try {
            init(dead, id);
            assertTrue("procedure never entered", procedure.entered.await(5_000, TimeUnit.MILLISECONDS));
            Thread deadSessionThread = findThread("Pedro-Tuning");

            // Drive the op mode past its confirmation, then go silent forever.
            confirm(dead, 1);
            assertTrue("op mode never became active",
                    awaitTrue(5_000, () -> startCount.get() == 1
                            && procedure.thread != null
                            && procedure.thread.getState() == Thread.State.WAITING));

            // Takeover: probe times out, claim is stolen, old session aborted.
            TestClient newcomer = new TestClient(port);
            try {
                newcomer.connectOrThrow(5_000);
                // Admission can transiently race the old session's teardown;
                // retry models the browser reload.
                newcomer.initUntilAccepted(id, 5_000);

                joinThread(deadSessionThread, 5_000);
                assertEquals("op mode stop called more than once", 1, stopCount.get());
                assertEquals(server.sockets.get(1), WebSocketServer.activeSocket());
            } finally {
                newcomer.closeOrThrow(2_000);
            }
        } finally {
            dead.close();
        }
    }
}
