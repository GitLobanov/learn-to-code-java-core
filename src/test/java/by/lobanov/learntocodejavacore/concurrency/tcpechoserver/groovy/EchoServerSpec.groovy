package by.lobanov.learntocodejavacore.concurrency.tcpechoserver.groovy

import by.lobanov.learntocodejavacore.concurrency.tcpechoserver.EchoServer
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * This test in groovy just for own education
 */
class EchoServerSpec extends Specification {

    @Shared
    Thread serverThread

    @Shared
    def serverPort = 12345

    def setupSpec() {
        serverThread = new Thread({ new EchoServer(serverPort).start() })
        serverThread.start()
        Thread.sleep(500) // Wait for server to start
    }

    def cleanupSpec() {
        serverThread?.interrupt()
    }

    def "should echo back the same message"() {
        given: "a client connection to the server"
        def socket = new Socket("localhost", serverPort)
        def out = new PrintWriter(socket.getOutputStream(), true)
        def input = new BufferedReader(new InputStreamReader(socket.getInputStream()))

        when: "client sends a message"
        def message = "Hello from Spock test!"
        out.println(message)

        then: "server echoes back the same message"
        def response = input.readLine()
        response == message

        cleanup:
        socket?.close()
    }

    def "should handle multiple concurrent clients"() {
        given: "multiple client connection"
        def clientCount = 3
        def executor = Executors.newFixedThreadPool(clientCount)
        def results = []

        when: "multiple clients send messages simultaneously"
        (1..clientCount).each { clientId ->
            executor.submit {
                def socket = new Socket("localhost", serverPort)
                def out = new PrintWriter(socket.getOutputStream(), true)
                def input = new BufferedReader(new InputStreamReader(socket.getInputStream()))

                def message = "Message from client $clientId"
                out.println(message)
                def response = input.readLine()

                synchronized (results) {
                    results << [sent: message, received: response]
                }
                socket.close()
            }
        }

        executor.shutdown()
        executor.awaitTermination(5L, TimeUnit.SECONDS)

        then: "all clients receive their echoed messages"
        results.size() == clientCount
        results.every { it.sent == it.received }
    }

    def "should close connection when client sends exit command"() {
        given: "a client connection"
        def socket = new Socket("localhost", serverPort)
        def out = new PrintWriter(socket.getOutputStream(), true)
        def input = new BufferedReader(new InputStreamReader(socket.getInputStream()))

        when: "client sends exit command"
        out.println("exit")

        then: "server sends goodbye message and closes connection"
        def response = input.readLine()
        response == "Goodbye!"

        and: "connection is closed"
        input.readLine() == null

        cleanup:
        socket?.close()
    }

}
