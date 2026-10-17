package com.pedropathing.tuning.autotune;

import fi.iki.elonen.NanoWSD;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Server-side liveness watchdog for the tuning websocket.
 *
 * Two executors, deliberately split:
 * <ul>
 *     <li>{@code "Tune-Watchdog"}: a single daemon scheduler thread. It owns
 *     bookkeeping and kill decisions only and must never perform frame I/O
 *     itself: all frame writes funnel through {@code sendFrame}, which is
 *     {@code synchronized} on the socket and writes synchronously with a
 *     flush, so a {@code write()} can block for minutes under TCP
 *     backpressure on a half-dead link. If the scheduler thread itself sent
 *     pings, one stuck write would stall every future tick and silently kill
 *     the watchdog.</li>
 *     <li>{@code "Tune-Ping"}: a daemon pool that performs the actual
 *     {@code ping()} sends. The scheduler submits each ping and inspects the
 *     returned future on the <em>next</em> tick: a ping whose future is not
 *     done by then counts as a missed pong, and a ping whose future completed
 *     exceptionally (RST/EPIPE) is a definitive dead signal and triggers an
 *     immediate force-close.</li>
 * </ul>
 */
final class ConnectionWatchdog {
    /**
     * Timing constants are package-private and non-final so JVM tests can
     * scale them down to milliseconds. They must be set before {@link #start()}.
     *
     * <p>Production values:</p>
     * <ul>
     *     <li>{@link #PING_INTERVAL_MS} = 10 s: tuning stages idle for minutes;
     *     ping/pong frames are protocol-level and ~2-30 bytes, invisible on wifi.</li>
     *     <li>{@link #PONG_MISS_THRESHOLD} = 2 consecutive pongless intervals:
     *     detection 20-30 s after silent death. A single missed pong is routinely
     *     caused by a wifi blip or GC pause; a false kill aborts minutes of manual
     *     pushing (unrecoverable), while the extra interval only delays reclaiming
     *     an already-useless connection. Any pong resets the counter.</li>
     *     <li>{@link #PROBE_DEADLINE_MS} = 2.5 s for the takeover liveness probe:
     *     browser pongs answer sub-100 ms healthy, at most ~1 s on a crowded
     *     field, so this keeps rejection UX snappy with a ~5x margin.</li>
     * </ul>
     */
    static volatile long PING_INTERVAL_MS = 10_000L;
    static volatile int PONG_MISS_THRESHOLD = 2;
    static volatile long PROBE_DEADLINE_MS = 2_500L;

    /** Constant ping payload. Deliberately not correlated with pongs (any pong counts). */
    private static final byte[] PING_PAYLOAD = "AutoTune".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

    private static volatile ScheduledExecutorService scheduler;
    private static volatile ExecutorService pingExecutor;
    private static volatile ScheduledFuture<?> tickTask;

    private ConnectionWatchdog() {}

    static synchronized void start() {
        if (scheduler != null) return;

        ScheduledExecutorService newScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "Tune-Watchdog");
            thread.setDaemon(true);
            return thread;
        });
        ExecutorService newPingExecutor = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "Tune-Ping");
            thread.setDaemon(true);
            return thread;
        });

        scheduler = newScheduler;
        pingExecutor = newPingExecutor;
        long interval = PING_INTERVAL_MS;
        tickTask = newScheduler.scheduleAtFixedRate(
                ConnectionWatchdog::tick,
                interval,
                interval,
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Shuts the watchdog down before the servers stop. {@code shutdownNow} plus a
     * bounded await so no {@code "Tune-Watchdog"}/{@code "Tune-Ping"} threads
     * outlive the event loop.
     */
    static synchronized void stop() {
        ScheduledExecutorService stoppingScheduler = scheduler;
        ExecutorService stoppingPingExecutor = pingExecutor;
        tickTask = null;
        scheduler = null;
        pingExecutor = null;

        shutdown(stoppingScheduler);
        shutdown(stoppingPingExecutor);
    }

    /** Current ping executor, or {@code null} when the watchdog is not running. */
    static ExecutorService pingExecutor() {
        return pingExecutor;
    }

    /**
     * Submits a ping on the ping executor and records the send in the socket's
     * liveness bookkeeping. The returned future completes exceptionally if the
     * write fails, and may never complete if the write is stuck; both register
     * as liveness failures on a later tick (or, while the watchdog is disabled,
     * during a takeover probe where a failed send also counts as no-liveness).
     */
    static CompletableFuture<Void> submitPing(WebSocketServer.Socket socket) {
        socket.pendingSinceNanos = System.nanoTime();
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            ExecutorService executor = pingExecutor;
            if (executor == null) throw new RejectedExecutionException("watchdog stopped");
            executor.execute(() -> {
                try {
                    socket.ping(PING_PAYLOAD);
                    future.complete(null);
                } catch (IOException exception) {
                    future.completeExceptionally(exception);
                }
            });
        } catch (RejectedExecutionException exception) {
            future.completeExceptionally(exception);
        }
        socket.pingSendFuture = future;
        return future;
    }

    private static void tick() {
        try {
            WebSocketServer.Socket socket = WebSocketServer.activeSocket();
            if (socket == null) return;

            // A ping write that already failed is a definitive dead signal.
            CompletableFuture<Void> pendingPing = socket.pingSendFuture;
            if (pendingPing != null && pendingPing.isDone() && pendingPing.isCompletedExceptionally()) {
                WebSocketServer.forceClose(
                        socket,
                        NanoWSD.WebSocketFrame.CloseCode.AbnormalClosure,
                        "Ping write failed."
                );
                return;
            }

            long now = System.nanoTime();
            if (socket.pendingSinceNanos != 0
                    && now - socket.pendingSinceNanos >= TimeUnit.MILLISECONDS.toNanos(PING_INTERVAL_MS)) {
                // The previous ping is unanswered (or still in flight): a miss.
                // consecutiveMisses is confined to this single watchdog thread.
                socket.consecutiveMisses++;
                socket.pendingSinceNanos = 0;
                if (socket.consecutiveMisses >= PONG_MISS_THRESHOLD) {
                    WebSocketServer.forceClose(
                            socket,
                            NanoWSD.WebSocketFrame.CloseCode.GoingAway,
                            "Tuning client unresponsive."
                    );
                    return;
                }
            } else {
                socket.consecutiveMisses = 0;
            }

            submitPing(socket);
        } catch (Throwable throwable) {
            // The watchdog must never die: report and keep ticking.
            android.util.Log.e("ConnectionWatchdog", "watchdog tick failed", throwable);
        }
    }

    private static void shutdown(ExecutorService executor) {
        if (executor == null) return;
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                android.util.Log.w("ConnectionWatchdog", "executor did not terminate in time");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }
}
