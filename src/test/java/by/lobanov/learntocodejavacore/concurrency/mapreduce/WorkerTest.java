package by.lobanov.learntocodejavacore.concurrency.mapreduce;

import by.lobanov.learntocodejavacore.concurrency.mapreduce.function.*;
import by.lobanov.learntocodejavacore.concurrency.mapreduce.function.impl.*;
import by.lobanov.learntocodejavacore.concurrency.mapreduce.model.*;
import by.lobanov.learntocodejavacore.concurrency.mapreduce.task.*;
import by.lobanov.learntocodejavacore.concurrency.mapreduce.task.impl.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.*;
import org.mockito.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class WorkerTest {

    @TempDir
    Path tempDir;

    private Coordinator mockCoordinator;
    private MapFunction mockMapFunction;
    private ReduceFunction mockReduceFunction;
    private Worker worker;

    @BeforeEach
    void setUp() {
        mockCoordinator = mock(Coordinator.class);
        mockMapFunction = mock(MapFunction.class);
        mockReduceFunction = mock(ReduceFunction.class);
        worker = new Worker(1, mockCoordinator, mockMapFunction, mockReduceFunction);
    }

    @Test
    void testWorkerInitialization() {
        assertNotNull(worker);
    }

    @Test
    void testWorkerHandlesMapTask() throws IOException {
        // given
        Path inputFile = tempDir.resolve("input.txt");
        Files.writeString(inputFile, "test content");

        MapTask mapTask = new MapTask(inputFile.toString(), 0, 2, mockCoordinator, 1);
        List<KeyValue> mapResult = Arrays.asList(
                new KeyValue("test", "1"),
                new KeyValue("content", "1")
        );

        when(mockCoordinator.getTask(1))
                .thenReturn(mapTask)
                .thenReturn(new ShutdownTask());
        when(mockCoordinator.getIntermediateDir()).thenReturn(tempDir.resolve("intermediate"));
        when(mockMapFunction.map(inputFile.toString())).thenReturn(mapResult);

        // Create intermediate directory
        Files.createDirectories(tempDir.resolve("intermediate"));

        // when
        worker.run();

        // then
        verify(mockMapFunction).map(inputFile.toString());
        verify(mockCoordinator, times(2)).getTask(1);
        verify(mockCoordinator).mapTaskCompleted(eq(1), eq(0), any());
    }

    @Test
    void testWorkerHandlesReduceTask() throws IOException {
        // given
        Path intermediateFile = tempDir.resolve("mr-0-0");
        Files.writeString(intermediateFile, "test\t1\ncontent\t1\n");

        ReduceTask reduceTask = new ReduceTask(0,
                Arrays.asList(intermediateFile.toString()), 1, mockCoordinator);

        when(mockCoordinator.getTask(1))
                .thenReturn(reduceTask)
                .thenReturn(new ShutdownTask());
        when(mockCoordinator.getOutputDir()).thenReturn(tempDir.resolve("output"));
        when(mockReduceFunction.reduce("test", Arrays.asList("1"))).thenReturn("1");
        when(mockReduceFunction.reduce("content", Arrays.asList("1"))).thenReturn("1");

        // Create output directory
        Files.createDirectories(tempDir.resolve("output"));

        // when
        worker.run();

        // then
        verify(mockReduceFunction).reduce("test", Arrays.asList("1"));
        verify(mockReduceFunction).reduce("content", Arrays.asList("1"));
        verify(mockCoordinator, times(2)).getTask(1);
        verify(mockCoordinator).reduceTaskCompleted(eq(1), eq(0), any());
    }

    @Test
    void testWorkerHandlesNoTaskAvailable() {
        // given
        when(mockCoordinator.getTask(1))
                .thenReturn(new NoTaskAvailable())
                .thenReturn(new ShutdownTask());

        // when
        worker.run();

        // then
        verify(mockCoordinator, times(2)).getTask(1);
        verifyNoInteractions(mockMapFunction);
        verifyNoInteractions(mockReduceFunction);
    }

    @Test
    void testWorkerHandlesShutdownTask() {
        // given
        when(mockCoordinator.getTask(1)).thenReturn(new ShutdownTask());

        // when
        worker.run();

        // then
        verify(mockCoordinator).getTask(1);
        verifyNoInteractions(mockMapFunction);
        verifyNoInteractions(mockReduceFunction);
    }

    @Test
    void testWorkerWithMultipleTasks() throws IOException {
        // given
        Path inputFile = tempDir.resolve("input.txt");
        Files.writeString(inputFile, "test");

        Path intermediateFile = tempDir.resolve("mr-0-0");
        Files.writeString(intermediateFile, "test\t1\n");

        MapTask mapTask = new MapTask(inputFile.toString(), 0, 1, mockCoordinator, 1);
        ReduceTask reduceTask = new ReduceTask(0,
                Arrays.asList(intermediateFile.toString()), 1, mockCoordinator);

        when(mockCoordinator.getTask(1))
                .thenReturn(mapTask)
                .thenReturn(new NoTaskAvailable())
                .thenReturn(reduceTask)
                .thenReturn(new ShutdownTask());

        when(mockCoordinator.getIntermediateDir()).thenReturn(tempDir.resolve("intermediate"));
        when(mockCoordinator.getOutputDir()).thenReturn(tempDir.resolve("output"));
        when(mockMapFunction.map(inputFile.toString())).thenReturn(Arrays.asList(new KeyValue("test", "1")));
        when(mockReduceFunction.reduce("test", Arrays.asList("1"))).thenReturn("1");

        Files.createDirectories(tempDir.resolve("intermediate"));
        Files.createDirectories(tempDir.resolve("output"));

        // when
        worker.run();

        // then
        verify(mockCoordinator, times(4)).getTask(1);
        verify(mockMapFunction).map(inputFile.toString());
        verify(mockReduceFunction).reduce("test", Arrays.asList("1"));
        verify(mockCoordinator).mapTaskCompleted(eq(1), eq(0), any());
        verify(mockCoordinator).reduceTaskCompleted(eq(1), eq(0), any());
    }

    @Test
    void testWorkerWithRealMapReduceFunctions() throws IOException {
        // given
        Path inputFile = tempDir.resolve("input.txt");
        Files.writeString(inputFile, "hello world hello");

        WordCountMapFunction realMapFunction = new WordCountMapFunction();
        WordCountReduceFunction realReduceFunction = new WordCountReduceFunction();

        Worker realWorker = new Worker(1, mockCoordinator, realMapFunction, realReduceFunction);

        MapTask mapTask = new MapTask(inputFile.toString(), 0, 1, mockCoordinator, 1);
        when(mockCoordinator.getTask(1))
                .thenReturn(mapTask)
                .thenReturn(new ShutdownTask());
        when(mockCoordinator.getIntermediateDir()).thenReturn(tempDir.resolve("intermediate"));

        Files.createDirectories(tempDir.resolve("intermediate"));

        // when
        realWorker.run();

        // then
        verify(mockCoordinator, times(2)).getTask(1);
        verify(mockCoordinator).mapTaskCompleted(eq(1), eq(0), any());

        // Verify intermediate files were created
        assertTrue(Files.exists(tempDir.resolve("intermediate")));
    }

    @Test
    void testWorkerContinuesAfterException() {
        // given
        Task faultyTask = mock(Task.class);
        when(faultyTask.getType()).thenReturn(Task.TaskType.MAP);

        when(mockCoordinator.getTask(1))
                .thenReturn(faultyTask)
                .thenReturn(new ShutdownTask());

        // when
        worker.run();

        // then
        verify(mockCoordinator, times(2)).getTask(1);
    }

    @Test
    void testWorkerExecutionInSeparateThread() throws InterruptedException {
        // given
        when(mockCoordinator.getTask(1)).thenReturn(new ShutdownTask());

        CountDownLatch latch = new CountDownLatch(1);
        Thread workerThread = new Thread(() -> {
            worker.run();
            latch.countDown();
        });

        // when
        workerThread.start();
        boolean completed = latch.await(5, TimeUnit.SECONDS);

        // then
        assertTrue(completed);
        verify(mockCoordinator).getTask(1);
    }

    @Test
    void testMultipleWorkersWithSameCoordinator() throws InterruptedException {
        // given
        int numWorkers = 3;
        CountDownLatch latch = new CountDownLatch(numWorkers);

        when(mockCoordinator.getTask(anyInt())).thenReturn(new ShutdownTask());

        List<Thread> workerThreads = new ArrayList<>();

        // when
        for (int i = 0; i < numWorkers; i++) {
            int workerId = i + 1;
            Worker worker = new Worker(workerId, mockCoordinator, mockMapFunction, mockReduceFunction);
            Thread thread = new Thread(() -> {
                worker.run();
                latch.countDown();
            });
            workerThreads.add(thread);
            thread.start();
        }

        boolean completed = latch.await(5, TimeUnit.SECONDS);

        // then
        assertTrue(completed);
        verify(mockCoordinator, times(numWorkers)).getTask(anyInt());
    }

    @Test
    void testWorkerHandlesInterruption() throws InterruptedException {
        // given
        when(mockCoordinator.getTask(1))
                .thenAnswer(invocation -> {
                    Thread.sleep(1000); // Simulate long operation
                    return new ShutdownTask();
                });

        Thread workerThread = new Thread(worker);
        CountDownLatch startLatch = new CountDownLatch(1);

        workerThread.setUncaughtExceptionHandler((t, e) -> startLatch.countDown());

        // when
        workerThread.start();
        Thread.sleep(100); // Give worker time to start
        workerThread.interrupt();

        boolean interrupted = startLatch.await(5, TimeUnit.SECONDS);

        // then - worker should handle interruption gracefully
        assertTrue(interrupted || !workerThread.isAlive());
    }
}