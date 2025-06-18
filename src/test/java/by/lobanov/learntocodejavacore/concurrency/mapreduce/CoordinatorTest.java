package by.lobanov.learntocodejavacore.concurrency.mapreduce;

import by.lobanov.learntocodejavacore.concurrency.mapreduce.function.*;
import by.lobanov.learntocodejavacore.concurrency.mapreduce.function.impl.*;
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

    public static final int NUM_REDUCE_TASKS = 2;
    @TempDir
    Path tempDir;

    private Coordinator coordinator;
    private List<String> inputFiles;
    public static final int FILES = 3;

    @BeforeEach
    void setUp() throws IOException {
        inputFiles = new ArrayList<>();
        for (int i = 0; i < FILES; i++) {
            Path inputFile = tempDir.resolve("input" + i + ".txt");
            Files.writeString(inputFile, "test content " + i);
            inputFiles.add(inputFile.toString());
        }

        coordinator = new Coordinator(inputFiles, NUM_REDUCE_TASKS);
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
        // given - complete all map tasks
        completeAllMapTasks();

        // get reduce tasks
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
        assertDoesNotThrow(() -> waitFuture.get(6, TimeUnit.SECONDS));
        assertTrue(coordinator.isDone());
    }

    @Test
    void testConcurrentTasksRetrieval() throws InterruptedException {
        // given
        int numWorkers = 5;

        // when - multiple workers try to get tasks concurrently,
        List<Task> retrievedTasks = Collections.synchronizedList(new ArrayList<>());
        ExecutorService executorService = Executors.newFixedThreadPool(numWorkers);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(numWorkers);

        for (int i = 0; i < numWorkers; i++) {
            final int workerId = i + 1;
            executorService.submit(() -> {
                try {
                    startLatch.await(); // Wait for all workers to be ready
                    Task task = coordinator.getTask(workerId);
                    retrievedTasks.add(task);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        Thread.sleep(100);
        executorService.shutdownNow();
        long mapTasks = retrievedTasks.stream().filter(t -> t instanceof MapTask).count();
        long noTaskAvailable = retrievedTasks.stream().filter(t -> t instanceof NoTaskAvailable).count();

        assertTrue(retrievedTasks.size() >= 3, "Expected at least 3 tasks, got: " + retrievedTasks.size());
        assertEquals(3, mapTasks, "Should have exactly 3 map tasks");
        assertTrue(noTaskAvailable <= 2, "Should have at most 2 NoTaskAvailable responses");
    }

    private void completeAllMapTasks() {
        Set<Integer> completedTasks = new HashSet<>();
        while (completedTasks.size() < 3) {
            for (int i = 0; i < 3; i++) {
                if (completedTasks.contains(i)) continue;

                Task task = coordinator.getTask(i + 1);
                if (task instanceof MapTask mapTask) {
                    List<String> intermediateFiles = Arrays.asList("mr-" + i + "-0", "mr-" + i + "-1");
                    coordinator.mapTaskCompleted(i + 1, mapTask.mapTaskId, intermediateFiles);
                    completedTasks.add(i);
                }
            }
        }
    }

    private void completeAllReduceTasks() {
        Set<Integer> completedTasks = new HashSet<>();
        while (completedTasks.size() < 2) {
            for (int i = 0; i < 2; i++) {
                if (completedTasks.contains(i)) continue;

                Task task = coordinator.getTask(i + 1);
                if (task instanceof ReduceTask reduceTask) {
                    coordinator.reduceTaskCompleted(i + 1, reduceTask.reduceTaskId, "output-" + i);
                    completedTasks.add(i);
                }
            }
        }
    }
}
