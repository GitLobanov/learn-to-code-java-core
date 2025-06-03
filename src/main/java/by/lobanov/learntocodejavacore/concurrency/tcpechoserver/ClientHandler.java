package by.lobanov.learntocodejavacore.concurrency.tcpechoserver;

import org.slf4j.*;

import java.io.*;
import java.net.*;
import java.nio.charset.*;

/**
 * ClientHandler class is responsible for handling communication with a single client.
 * It reads messages from the client, echoes them back, and logs the interaction.
 */
public class ClientHandler implements Runnable {

    private final Socket socket;
    private static final Logger log = LoggerFactory.getLogger(ClientHandler.class);

    public ClientHandler(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try (
             var inputStream = new InputStreamReader(socket.getInputStream());
             var reader = new BufferedReader(inputStream);
             var writer = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8);
        ) {
            String clientMessage;
            log.info("Client {}, connected to server", socket.getInetAddress());

            while ((clientMessage = reader.readLine()) != null) {

                if (clientMessage.equalsIgnoreCase("exit") || clientMessage.equalsIgnoreCase("quit")) {
                    writer.println("Goodbye!");
                    break;
                }

                log.info("From client {}, get message: {} ",socket.getInetAddress(), clientMessage);
                writer.println(clientMessage);
            }
        } catch (IOException e) {
            log.error("Ошибка при обработке клиента {}: {}" ,
                    (socket != null ? socket.getInetAddress().getHostAddress() : "N/A"), e.getMessage());
        } finally {
            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
            } catch (IOException e) {
                log.error("Error closing client socket", e);
            }
            log.info("Client {}, disconnected from server",
                    socket != null ? socket.getInetAddress().getHostAddress() : "N/A");
        }
    }
}
