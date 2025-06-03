package by.lobanov.learntocodejavacore.concurrency.tcpechoserver;

import org.slf4j.*;

import java.io.*;
import java.net.*;
import java.nio.charset.*;

/**
 * Simple TCP client for testing purposes.
 */
public class SimpleTcpClient {

    private static final String SERVER_ADDRESS = "127.0.0.1";
    private static final int SERVER_PORT = 12345;
    private static final Logger log = LoggerFactory.getLogger(SimpleTcpClient.class);

    public static void main(String[] args) {
        log.info("Starting TCP client...");
        log.info("For exit from connection type 'exit' or 'quit'");

        try (
                var socket = new Socket(SERVER_ADDRESS, SERVER_PORT);
                var out = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8);
                var in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                var stdIn = new BufferedReader(new InputStreamReader(System.in))
        ) {
            log.info("Connected to server {}:{}", SERVER_ADDRESS, SERVER_PORT);
            String userInput;
            String serverResponse;

            while (true) {
                log.info("Enter message to server:");
                userInput = stdIn.readLine();
                out.println(userInput);

                if ("exit".equalsIgnoreCase(userInput) || "quit".equalsIgnoreCase(userInput)) {
                    log.info("Exiting...");
                    break;
                }

                serverResponse = in.readLine();
                if (serverResponse == null) {
                    log.info("Server closed connection");
                    break;
                }
                log.info("Server response: {}", serverResponse);
            }

        } catch (UnknownHostException e) {
            log.error("Unknown host: {}", SERVER_ADDRESS, e);
        } catch (IOException e) {
            log.error("I/O error occurred: {}", e.getMessage(), e);
        } finally {
            log.info("Client finished work.");
        }
    }
}
