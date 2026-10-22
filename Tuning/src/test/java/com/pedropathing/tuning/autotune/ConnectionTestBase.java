package com.pedropathing.tuning.autotune;

import fi.iki.elonen.NanoHTTPD;
import fi.iki.elonen.NanoWSD;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.Assert.assertTrue;

/**
 * Shared infrastructure for the JVM connection-reclamation tests: real
 * sockets on localhost, well-behaved clients backed by Java-WebSocket, a
 * raw-socket silent-death client, and helpers for the message flows.
 */
abstract class ConnectionTestBase {

    /** Server that captures the Socket instances it creates so tests can compare identities. */
    static final class CapturingServer extends WebSocketServer {
        final List<WebSocketServer.Socket> sockets = new CopyOnWriteArrayList<>();
        private final Function<NanoHTTPD.IHTTPSession, WebSocketServer.Socket> factory;

        CapturingServer(int port, Function<NanoHTTPD.IHTTPSession, WebSocketServer.Socket> factory) {
            super(port);
            this.factory = factory;
        }

        @Override
        protected NanoWSD.WebSocket openWebSocket(NanoHTTPD.IHTTPSession handshake) {
            WebSocketServer.Socket socket = factory.apply(handshake);
            sockets.add(socket);
            return socket;
        }
    }

    /** Well-behaved websocket client (answers pings) with a message queue. */
    static final class TestClient extends WebSocketClient implements AutoCloseable {
        final BlockingQueue<String> messages = new LinkedBlockingQueue<>();
        /** Non-matching messages seen while awaiting another kind; re-checked first. */
        final List<String> deferred = new ArrayList<>();
        final CountDownLatch openLatch = new CountDownLatch(1);
        final CountDownLatch closeLatch = new CountDownLatch(1);
        volatile Integer closeCode;
        volatile String closeReason;

        TestClient(int port) {
            super(URI.create("ws://localhost:" + port));
        }

        @Override
        public void onOpen(ServerHandshake handshake) {
            openLatch.countDown();
        }

        @Override
        public void onMessage(String message) {
            messages.add(message);
        }

        @Override
        public void onClose(int code, String reason, boolean remote) {
            closeCode = code;
            closeReason = reason;
            closeLatch.countDown();
        }

        @Override
        public void onError(Exception ex) {
        }

        void connectOrThrow(long timeoutMs) throws InterruptedException {
            assertTrue("client failed to connect", connectBlocking(timeoutMs, TimeUnit.MILLISECONDS));
        }

        void closeOrThrow(long timeoutMs) throws InterruptedException {
            close();
            closeLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
        }

        /**
         * Sends init until the server answers with the init response. A takeover
         * admission can transiently race the previous session's teardown
         * (currentSession is cleared by the old Pedro-Tuning thread's finally),
         * in which case the first init bounces with a "still running" error;
         * a retry models the browser reload.
         */
        void initUntilAccepted(String procedureId, long timeoutMs) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            while (System.nanoTime() < deadline) {
                send("{\"type\":\"init\",\"id\":\"" + procedureId + "\"}");
                String response = awaitMessageContaining("\"type\":\"", 400);
                if (response != null && response.contains("\"type\":\"init\"")) return;
                // Any other reply (e.g. the transient error) -> retry.
                messages.clear();
                deferred.clear();
            }
            org.junit.Assert.fail("init never accepted within " + timeoutMs + "ms");
        }

        /** Awaits a message containing {@code needle}, deferring non-matching ones. */
        String awaitMessageContaining(String needle, long timeoutMs) throws InterruptedException {
            for (int i = 0; i < deferred.size(); i++) {
                if (deferred.get(i).contains(needle)) return deferred.remove(i);
            }
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            while (true) {
                long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                if (remaining <= 0) return null;
                String message = messages.poll(remaining, TimeUnit.MILLISECONDS);
                if (message == null) return null;
                if (message.contains(needle)) return message;
                deferred.add(message);
            }
        }

        int awaitClose(long timeoutMs) throws InterruptedException {
            closeLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
            Integer code = closeCode;
            return code == null ? -1 : code;
        }
    }

    /**
     * Raw-socket websocket client for the silent-death failure class: performs
     * the upgrade handshake by hand, then never reads again (so it never
     * answers server pings) while optionally staying open (true silent death)
     * or RST-aborting the stream.
     */
    static final class RawClient implements Closeable {
        private final Socket socket;
        private final OutputStream out;
        private final InputStream in;

        RawClient(int port) throws IOException {
            this.socket = new Socket("localhost", port);
            this.out = socket.getOutputStream();
            this.in = socket.getInputStream();

            byte[] key = new byte[16];
            new Random().nextBytes(key);
            String request = "GET / HTTP/1.1\r\n"
                    + "Host: localhost:" + port + "\r\n"
                    + "Upgrade: websocket\r\n"
                    + "Connection: Upgrade\r\n"
                    + "Sec-WebSocket-Key: " + java.util.Base64.getEncoder().encodeToString(key) + "\r\n"
                    + "Sec-WebSocket-Version: 13\r\n\r\n";
            out.write(request.getBytes(StandardCharsets.US_ASCII));
            out.flush();

            String response = readHandshakeResponse();
            if (!response.startsWith("HTTP/1.1 101")) {
                throw new IOException("websocket handshake rejected: " + response);
            }
        }

        private String readHandshakeResponse() throws IOException {
            StringBuilder builder = new StringBuilder();
            while (!builder.toString().contains("\r\n\r\n")) {
                int read = in.read();
                if (read < 0) throw new EOFException("stream ended during handshake");
                builder.append((char) read);
            }
            return builder.toString();
        }

        /** Sends a masked text frame (clients must mask; the payload is ASCII JSON). */
        void sendText(String text) throws IOException {
            byte[] payload = text.getBytes(StandardCharsets.UTF_8);
            ByteArrayOutputStream frame = new ByteArrayOutputStream();
            frame.write(0x81);
            if (payload.length < 126) {
                frame.write(0x80 | payload.length);
            } else {
                frame.write(0x80 | 126);
                frame.write((payload.length >> 8) & 0xFF);
                frame.write(payload.length & 0xFF);
            }
            byte[] mask = new byte[4];
            new Random().nextBytes(mask);
            frame.write(mask);
            for (int i = 0; i < payload.length; i++) {
                frame.write(payload[i] ^ mask[i & 3]);
            }
            out.write(frame.toByteArray());
            out.flush();
        }

        /** Abrupt death: close with SO_LINGER 0 so the close sends an RST. */
        void rstClose() throws IOException {
            socket.setSoLinger(true, 0);
            socket.close();
        }

        /**
         * Blocks until the server side closes the stream (FIN or RST). Returns
         * false only on read timeout.
         */
        boolean awaitServerClose(long timeoutMs) throws IOException {
            socket.setSoTimeout((int) timeoutMs);
            try {
                in.read();
                return true;
            } catch (java.net.SocketTimeoutException timeout) {
                return false;
            } catch (IOException reset) {
                return true;
            }
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    /** Test procedure that records its Pedro-Tuning thread and parks in a confirmation. */
    static final class BlockingProcedure extends Procedure {
        final CountDownLatch entered = new CountDownLatch(1);
        volatile Thread thread;

        BlockingProcedure(String name) {
            super(name, "test procedure");
        }

        @Override
        public void run() throws InterruptedException {
            thread = Thread.currentThread();
            entered.countDown();
            confirmation("Confirm", "please confirm");
        }
    }

    /** Test procedure that parks in an active op mode after one confirmation. */
    static final class OpModeProcedure extends Procedure {
        final CountDownLatch entered = new CountDownLatch(1);
        volatile Thread thread;
        final TuningOpMode<Object> opMode = new TuningOpMode<Object>("Test OpMode", "test", true) {
            @Override
            protected Object runTuningOpMode() {
                return null;
            }
        };

        OpModeProcedure(String name) {
            super(name, "op mode test procedure");
        }

        @Override
        public void run() throws InterruptedException {
            thread = Thread.currentThread();
            entered.countDown();
            runOpMode(opMode);
        }
    }

    CapturingServer server;
    int port;

    @Before
    public final void baseSetUp() {
        // Static state is global: reset it for every test.
        ConnectionWatchdog.stop();
        WebSocketServer.clearForTest();
        Hooks.setOpModeRunner(null);
        ConnectionWatchdog.PING_INTERVAL_MS = 10_000L;
        ConnectionWatchdog.PONG_MISS_THRESHOLD = 2;
        ConnectionWatchdog.PROBE_DEADLINE_MS = 2_500L;
    }

    @After
    public final void baseTearDown() throws Exception {
        ConnectionWatchdog.stop();
        // Stop the server BEFORE the final reset: NanoWSD connection threads
        // must not process queued client messages into new sessions once the
        // test is over. (Tests that already stopped it: NanoHTTPD tolerates a
        // repeated stop via the try/catch.)
        if (server != null) {
            try {
                server.stop();
            } catch (RuntimeException ignored) {
                // already stopped by the test body
            }
            server = null;
        }
        Hooks.setOpModeRunner(null);
        // The global session registry must actually be free before the next
        // test: a session that is still unwinding (or was begun late by a
        // lingering reader thread) makes beginProcedure return null and would
        // poison the next class. Probe beginProcedure directly (package-private
        // seam) and only pass once it admits a probe session, which is then
        // aborted again.
        assertTrue("tuning session registry never drained after teardown",
                awaitTrue(15_000, ConnectionTestBase::probeSessionRegistryFree));
        assertTrue("Pedro-Tuning threads survived teardown",
                awaitTrue(10_000, () -> findThread("Pedro-Tuning") == null));
    }

    private static boolean probeSessionRegistryFree() {
        WebSocketServer.clearForTest();
        Procedure probe = new Procedure("probe", "teardown probe") {
            @Override
            public void run() {
            }
        };
        TunerRegistrar.register("probe", probe);
        int count = TunerRegistrar.getProcedures().size();
        String id = TunerRegistrar.getProcedures().get(count - 1).id;
        TuningSession session = TuningSession.beginProcedure(id, DEAD_TRANSPORT);
        if (session == null) return false;
        WebSocketServer.clearForTest();
        return true;
    }

    private static final TuningSession.Transport DEAD_TRANSPORT = new TuningSession.Transport() {
        @Override
        public void requestConfirmation(long requestId, String title, String message, Display display) {
        }

        @Override
        public void requestInputs(long requestId, Inputs inputs, Display display) {
        }

        @Override
        public void opModeRunning(long requestId, boolean canStop, String name, Display display) {
        }

        @Override
        public void complete(java.util.Map<String, String> results, java.util.Map<Object, String> resultCode) {
        }

        @Override
        public void error(String message) {
        }
    };

    void startServer(long pingIntervalMs, int pongMissThreshold, long probeDeadlineMs) throws IOException {
        startServer(pingIntervalMs, pongMissThreshold, probeDeadlineMs,
                handshake -> new WebSocketServer.Socket(handshake));
    }

    void startServer(
            long pingIntervalMs,
            int pongMissThreshold,
            long probeDeadlineMs,
            Function<NanoHTTPD.IHTTPSession, WebSocketServer.Socket> factory
    ) throws IOException {
        ConnectionWatchdog.PING_INTERVAL_MS = pingIntervalMs;
        ConnectionWatchdog.PONG_MISS_THRESHOLD = pongMissThreshold;
        ConnectionWatchdog.PROBE_DEADLINE_MS = probeDeadlineMs;
        server = new CapturingServer(0, factory);
        server.start(3_600_000);
        port = server.getListeningPort();
        assertTrue(port > 0);
        ConnectionWatchdog.start();
    }

    String register(Procedure procedure) {
        TunerRegistrar.register("test", procedure);
        return TunerRegistrar.getProcedures().get(0).id;
    }

    static void init(TestClient client, String procedureId) throws InterruptedException {
        client.send("{\"type\":\"init\",\"id\":\"" + procedureId + "\"}");
    }

    static void init(RawClient client, String procedureId) throws IOException {
        client.sendText("{\"type\":\"init\",\"id\":\"" + procedureId + "\"}");
    }

    static void confirm(TestClient client, long requestId) {
        client.send("{\"type\":\"confirm\",\"requestId\":" + requestId + "}");
    }

    static void confirm(RawClient client, long requestId) throws IOException {
        client.sendText("{\"type\":\"confirm\",\"requestId\":" + requestId + "}");
    }

    static long requestIdOf(String message) {
        java.util.regex.Matcher matcher = java.util.regex.Pattern
                .compile("\"requestId\":(\\d+)")
                .matcher(message);
        assertTrue("no requestId in: " + message, matcher.find());
        return Long.parseLong(matcher.group(1));
    }

    static boolean awaitTrue(long timeoutMs, Callable<Boolean> condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < deadline) {
            if (Boolean.TRUE.equals(condition.call())) return true;
            Thread.sleep(20);
        }
        return Boolean.TRUE.equals(condition.call());
    }

    static Thread findThread(String name) {
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.getName().equals(name)) return thread;
        }
        return null;
    }

    static void assertNoNamedThreads(String name, long timeoutMs) throws Exception {
        assertTrue("thread(s) named " + name + " still alive", awaitTrue(timeoutMs, () -> findThread(name) == null));
    }

    static void joinThread(Thread thread, long timeoutMs) throws InterruptedException {
        if (thread == null) return;
        thread.join(timeoutMs);
        if (thread.isAlive()) {
            StringBuilder trace = new StringBuilder("thread did not exit in time: ")
                    .append(thread.getName()).append(" state=").append(thread.getState()).append('\n');
            for (StackTraceElement element : thread.getStackTrace()) {
                trace.append("  at ").append(element).append('\n');
            }
            org.junit.Assert.fail(trace.toString());
        }
    }
}
