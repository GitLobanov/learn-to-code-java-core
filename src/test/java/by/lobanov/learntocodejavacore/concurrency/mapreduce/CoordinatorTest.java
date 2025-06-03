package by.lobanov.learntocodejavacore.concurrency.mapreduce;

import by.lobanov.learntocodejavacore.concurrency.mapreduce.task.*;
import by.lobanov.learntocodejavacore.concurrency.mapreduce.task.impl.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class CoordinatorTest {

    @TempDir
    Path tempDir;

    private Coordinator coordinator;
    private List<String> inputFiles;

    @BeforeEach
    void setUp() throws IOException {
        inputFiles = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Path inputFile = tempDir.resolve("input" + i + ".txt");
            Files.writeString(inputFile, "test content " + i);
            inputFiles.add(inputFile.toString());
        }

        coordinator = new Coordinator(inputFiles, 2);
    }

    @Test
    void testCoordinatorInitialization() {
        assertNotNull(coordinator);
        assertNotNull(coordinator.getIntermediateDir());
        assertNotNull(coordinator.getOutputDir());
        assertTrue(Files.exists(coordinator.getIntermediateDir()));
        assertTrue(Files.exists(coordinator.getOutputDir()));
        assertFalse(coordinator.isDone());
    }

    @Test
    void testGetMapTask() {
        // given
        int workerId = 1;

        // when
        Task task = coordinator.getTask(workerId);

        // then
        assertInstanceOf(MapTask.class, task);
        MapTask mapTask = (MapTask) task;
        assertTrue(inputFiles.contains(mapTask.inputFile));
        assertEquals(0, mapTask.mapTaskId);
        assertEquals(2, mapTask.numReduceTasks);
    }

    @Test
    void testGetMultipleMapTasks() {
        // when
        Task task1 = coordinator.getTask(1);
        Task task2 = coordinator.getTask(2);
        Task task3 = coordinator.getTask(3);

        // then
        assertInstanceOf(MapTask.class, task1);
        assertInstanceOf(MapTask.class, task2);
        assertInstanceOf(MapTask.class, task3);

        MapTask mapTask1 = (MapTask) task1;
        MapTask mapTask2 = (MapTask) task2;
        MapTask mapTask3 = (MapTask) task3;

        assertEquals(0, mapTask1.mapTaskId);
        assertEquals(1, mapTask2.mapTaskId);
        assertEquals(2, mapTask3.mapTaskId);
    }

    @Test
    void testNoTaskAvailableWhenAllMapTasksDistributed() {
        // given - get all map tasks
        coordinator.getTask(1);
        coordinator.getTask(2);
        coordinator.getTask(3);

        // when - try to get another task
        Task task = coordinator.getTask(4);

        // then
        assertInstanceOf(NoTaskAvailable.class, task);
    }

    @Test
    void testMapTaskCompletion() {
        // given
        Task task = coordinator.getTask(1);
        MapTask mapTask = (MapTask) task;
        List<String> intermediateFiles = Arrays.asList("mr-0-0", "mr-0-1");

        // when
        coordinator.mapTaskCompleted(1, mapTask.mapTaskId, intermediateFiles);

        // then
        assertFalse(coordinator.isDone());
    }

    @Test
    void testDuplicateMapTaskCompletion() {
        // given
        Task task = coordinator.getTask(1);
        MapTask mapTask = (MapTask) task;
        List<String> intermediateFiles = Arrays.asList("mr-0-0", "mr-0-1");

        // when
        coordinator.mapTaskCompleted(1, mapTask.mapTaskId, intermediateFiles);
        coordinator.mapTaskCompleted(1, mapTask.mapTaskId, intermediateFiles); // duplicate

        // then - should not cause issues
        assertFalse(coordinator.isDone());
    }

    @Test
    void testReduceTaskAfterAllMapTasksCompleted() {
        // given - complete all map tasks
        for (int i = 0; i < 3; i++) {
            Task task = coordinator.getTask(i + 1);
            MapTask mapTask = (MapTask) task;
            List<String> intermediateFiles = Arrays.asList("mr-" + i + "-0", "mr-" + i + "-1");
            coordinator.mapTaskCompleted(i + 1, mapTask.mapTaskId, intermediateFiles);
        }

        // when
        Task reduceTask = coordinator.getTask(1);

        // then
        assertInstanceOf(ReduceTask.class, reduceTask);
        ReduceTask task = (ReduceTask) reduceTask;
        assertTrue(task.reduceTaskId >= 0 && task.reduceTaskId < 2);
    }

    @Test
    void testReduceTaskCompletion() {
        // given - complete all map tasks first
        completeAllMapTasks();

        // get reduce task
        Task task = coordinator.getTask(1);
        ReduceTask reduceTask = (ReduceTask) task;

        // when
        coordinator.reduceTaskCompleted(1, reduceTask.reduceTaskId, "output-file-" + reduceTask.reduceTaskId);

        // then
        assertFalse(coordinator.isDone());
    }

    @Test
    void testDuplicateReduceTaskCompletion() {
        // given
        completeAllMapTasks();
        Task task = coordinator.getTask(1);
        ReduceTask reduceTask = (ReduceTask) task;

        // when
        coordinator.reduceTaskCompleted(1, reduceTask.reduceTaskId, "output-file");
        coordinator.reduceTaskCompleted(1, reduceTask.reduceTaskId, "output-file"); // duplicate

        // then - should not cause issues
        assertFalse(coordinator.isDone());
    }

    @Test
    void testShutdownTaskAfterAllTasksCompleted() {
        // given - complete all tasks
        completeAllMapTasks();
        completeAllReduceTasks();

        // when
        Task task = coordinator.getTask(1);

        // then
        assertInstanceOf(ShutdownTask.class, task);
        assertTrue(coordinator.isDone());
    }

    @Test
    void testIsDoneOnlyWhenAllTasksCompleted() {

        label:
        while (true) {
            Task task = coordinator.getTask(1);
            switch (task) {
                case MapTask mapTask:
                    coordinator.mapTaskCompleted(1, mapTask.mapTaskId, Arrays.asList("mr-0-0", "mr-0-1"));
                    break;
                case ReduceTask reduceTask:
                    coordinator.reduceTaskCompleted(3, reduceTask.reduceTaskId, "output-file");
                    break;
                case null, default:
                    break label;
            }
        }

        completeAllMapTasks();
        completeAllReduceTasks();

        assertTrue(coordinator.isDone());
    }

    @Test
    void testWaitUntilAllDoneWithTimeout() {
        // given
        CompletableFuture<Void> waitFuture = CompletableFuture.runAsync(() -> coordinator.waitUntilAllDone());

        // when - complete all tasks in another thread
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(100);
                completeAllMapTasks();
                Thread.sleep(100);
                completeAllReduceTasks();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // then
        assertDoesNotThrow(() -> waitFuture.get(5, TimeUnit.SECONDS));
        assertTrue(coordinator.isDone());
    }

    @Test
    void testConcurrentTaskRetrieval() throws InterruptedException {
        // given
        int numWorkers = 5;
        CountDownLatch latch = new CountDownLatch(numWorkers);
        List<Task> retrievedTasks = Collections.synchronizedList(new ArrayList<>());

        // when - multiple workers try to get tasks concurrently
        for (int i = 0; i < numWorkers; i++) {
            final int workerId = i + 1;
            new Thread(() -> {
                try {
                    Task task = coordinator.getTask(workerId);
                    retrievedTasks.add(task);
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await(5, TimeUnit.SECONDS);

        // then
        assertEquals(numWorkers, retrievedTasks.size());
        long mapTasks = retrievedTasks.stream().filter(t -> t instanceof MapTask).count();
        long noTaskAvailable = retrievedTasks.stream().filter(t -> t instanceof NoTaskAvailable).count();

        assertEquals(3, mapTasks);
        assertEquals(2, noTaskAvailable);
    }

    private void completeAllMapTasks() {
        for (int i = 0; i < 3; i++) {
            Task task = coordinator.getTask(i + 1);
            if (task instanceof MapTask mapTask) {
                List<String> intermediateFiles = Arrays.asList("mr-" + i + "-0", "mr-" + i + "-1");
                coordinator.mapTaskCompleted(i + 1, mapTask.mapTaskId, intermediateFiles);
            }
        }
    }

    private void completeAllReduceTasks() {
        for (int i = 0; i < 2; i++) {
            Task task = coordinator.getTask(i + 1);
            if (task instanceof ReduceTask reduceTask) {
                coordinator.reduceTaskCompleted(i + 1, reduceTask.reduceTaskId, "output-" + i);
            }
        }
    }
}
