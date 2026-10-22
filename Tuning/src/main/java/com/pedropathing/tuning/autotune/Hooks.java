package com.pedropathing.tuning.autotune;

import com.qualcomm.robotcore.eventloop.opmode.OpModeManagerImpl;
import dev.frozenmilk.sinister.sdk.apphooks.OnCreateEventLoop;
import dev.frozenmilk.sinister.sdk.apphooks.OnDestroy;
import dev.frozenmilk.sinister.sdk.opmodes.SinisterRegisteredOpModes;
import org.firstinspires.ftc.robotcore.internal.opmode.OpModeMeta;

import java.io.IOException;

@SuppressWarnings("unused")
class Hooks {

    private static final Object serverLock = new Object();

    @SuppressWarnings("StaticFieldLeak")
    private static volatile OpModeManagerImpl opModeManager;
    private static volatile WebServer server;
    private static volatile WebSocketServer wsServer;
    private static final OnCreateEventLoop onCreateEventLoop = (context, eventLoop) ->
    {
        synchronized (serverLock) {
            stop();
            opModeManager = eventLoop.getOpModeManager();
            ImageRegistrar.assets = context.getAssets();
            server = new WebServer(context.getAssets());
            wsServer = new WebSocketServer();
            try {
                server.start();
                wsServer.start(Integer.MAX_VALUE);
                ConnectionWatchdog.start();
            } catch (IOException e) {
                stop();
                throw new RuntimeException(e);
            }
        }
    };

    private static final OnDestroy onDestroy = context -> {
        synchronized (serverLock) {
            stop();
            opModeManager = null;
        }
    };

    private static void stop() {
        TuningSession.abort();
        // Shut the watchdog down before the servers: no tick may fire
        // force-close on a socket whose server is already stopping, and no
        // watchdog threads may outlive the event loop.
        ConnectionWatchdog.stop();
        if (wsServer != null) {
            wsServer.stop();
            wsServer = null;
        }
        if (server != null) {
            server.stop();
            server = null;
        }
    }

    static OpModeManagerImpl getOpModeManager() {
        return opModeManager;
    }

    /**
     * Seam for JVM tests: the FTC {@link OpModeManagerImpl} and Sinister calls
     * are not usable on the JVM, so {@link Procedure#runOpMode} routes its op
     * mode lifecycle through this interface. The default implementation is the
     * exact production behavior; tests override the instance.
     */
    interface OpModeRunner {
        void register(OpModeMeta meta, TuningOpMode<?> opMode);

        void init(OpModeMeta meta);

        void startActiveOpMode();

        void requestOpModeStop(TuningOpMode<?> opMode);

        void unregister(OpModeMeta meta);
    }

    private static volatile OpModeRunner opModeRunner;

    static OpModeRunner getOpModeRunner() {
        OpModeRunner runner = opModeRunner;
        if (runner == null) {
            runner = defaultOpModeRunner();
            opModeRunner = runner;
        }
        return runner;
    }

    // Test seam: install a recording/fake runner.
    static void setOpModeRunner(OpModeRunner runner) {
        opModeRunner = runner;
    }

    private static OpModeRunner defaultOpModeRunner() {
        return new OpModeRunner() {
            @Override
            public void register(OpModeMeta meta, TuningOpMode<?> opMode) {
                SinisterRegisteredOpModes.INSTANCE.register(meta, opMode);
            }

            @Override
            public void init(OpModeMeta meta) {
                opModeManager.initOpMode(meta.name);
            }

            @Override
            public void startActiveOpMode() {
                opModeManager.startActiveOpMode();
            }

            @Override
            public void requestOpModeStop(TuningOpMode<?> opMode) {
                opModeManager.requestOpModeStop(opMode);
            }

            @Override
            public void unregister(OpModeMeta meta) {
                SinisterRegisteredOpModes.INSTANCE.unregister(meta);
            }
        };
    }
}
