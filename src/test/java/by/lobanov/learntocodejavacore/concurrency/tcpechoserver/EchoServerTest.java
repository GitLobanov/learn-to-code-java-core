package by.lobanov.learntocodejavacore.concurrency.tcpechoserver;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

import java.io.*;
import java.net.*;
import java.nio.charset.*;
import java.util.concurrent.*;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class EchoServerTest {

    private EchoServer echoServer;
    private static final int SERVER_PORT = 12345;
    private ExecutorService executorService;

    @BeforeEach
    void setUp() {
        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            echoServer = new EchoServer(SERVER_PORT);
            echoServer.start();
        });

        // waiting for server to start
        try {
            sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void whenConnectAndSendMessge_thenShouldEchoMessage() throws IOException {
        try (var socket = new Socket("localhost", SERVER_PORT);
             var out = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8);
             var in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            String testMessage = "Hello, Server!";
            out.println(testMessage);

            String response = in.readLine();
            assertEquals(testMessage, response);
        }
    }
    @Test
    void whenCreateMultipleClintConnections_thenShouldHandleThemeProperly() throws InterruptedException {
        var clientCount = 5;
        var latch = new CountDownLatch(clientCount);
        var clientExecutor = Executors.newFixedThreadPool(clientCount);

        for (int i = 0; i < clientCount; i++) {
            final int clientId = i;

            clientExecutor.submit(() -> {
                try (var socket = new Socket("localhost", SERVER_PORT);
                     var out = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8);
                     var in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
                ) {
                    String message = "Hello from client " + clientId;
                    out.println(message);
                    String response = in.readLine();
                    assertEquals(message, response);
                } catch (IOException e) {
                    fail("Client " + clientId + " failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        clientExecutor.shutdown();
    }

    @Test
    void whenSendLargeMessage_thenShouldHandleIt() {
        try (var socket = new Socket("localhost", SERVER_PORT);
             var out = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8);
             var in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            var largeMessage = "a".repeat(1000);
            out.println(largeMessage);
            String response = in.readLine();
            assertEquals(largeMessage, response);
        } catch (IOException e) {
            fail("Failed to send/receive large message: " + e.getMessage());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "Test message", "12345", "!@#$%^&*()_+"})
    void whenSendDifferentTypeOfMessages_thenShouldHandleThem(String message){
        try (var socket = new Socket("localhost", SERVER_PORT);
             var out = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8);
             var in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
        ) {
            out.println(message);
            String response = in.readLine();
            assertEquals(message, response);
        } catch (IOException e) {
            fail("Failed to send/receive message: " + message + "\n" + e.getMessage());
        }
    }

    @AfterEach
    void tearDown() {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }
}
