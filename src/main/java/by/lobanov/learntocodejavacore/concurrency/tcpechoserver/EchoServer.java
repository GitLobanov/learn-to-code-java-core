package by.lobanov.learntocodejavacore.concurrency.tcpechoserver;

import org.slf4j.*;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;

/**
 * TCP Echo Server
 * Have method start() for starting server
 * Method main() for starting server from command line
 */
public class EchoServer {

    private static final Logger log = LoggerFactory.getLogger(EchoServer.class);
    private int port = 12345;

    public EchoServer(int port) {
        this.port = port;
    }

    public EchoServer() {
    }

    @SuppressWarnings("all")
    public void start() {
        log.info("Starting EchoServer on port {}", port);

        try (
            var serverExecutor = Executors.newCachedThreadPool();
            var serverSocket = new ServerSocket(port)
        ) {
            while (true) {
                log.info("Waiting for client connection...");
                var socket = serverSocket.accept();
                log.info("Client connected: {}", socket.getInetAddress());

                serverExecutor.submit(new ClientHandler(socket));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        EchoServer echoServer = new EchoServer();
        echoServer.start();
    }
}
