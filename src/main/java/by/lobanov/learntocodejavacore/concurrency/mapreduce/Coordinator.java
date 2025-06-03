package by.lobanov.learntocodejavacore.concurrency.mapreduce;

import by.lobanov.learntocodejavacore.concurrency.mapreduce.task.*;
import by.lobanov.learntocodejavacore.concurrency.mapreduce.task.impl.*;
import lombok.*;
import org.slf4j.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class Coordinator {

    private final Logger log = LoggerFactory.getLogger(Coordinator.class);

    private final List<String> inputFiles;
    private final int numReduceTasks;

    private final Queue<String> pendingMapInputFiles; // Файлы, ожидающие обработки map-функцией
    private final AtomicInteger nextMapTaskId = new AtomicInteger(0);
    private final Set<Integer> completedMapTaskIds = Collections.synchronizedSet(new HashSet<>());
    private final int totalMapTasks;

    private final Map<Integer, List<String>> mapTaskOutputFiles = new ConcurrentHashMap<>();

    private final Queue<Integer> pendingReduceTaskIds = new LinkedList<>();
    private final Set<Integer> completedReduceTaskIds = Collections.synchronizedSet(new HashSet<>());

    private volatile boolean allMapTasksReportedDone = false;
    private volatile boolean reduceTasksPrepared = false; // Указывает, что reduce задачи готовы к раздаче
    private volatile boolean allReduceTasksReportedDone = false;

    private final Object lock = new Object(); // Объект для синхронизации и wait/notify

    private final String intermediateDirName = "intermediate_files";
    private final String outputDirName = "output_files";
    @Getter
    private final Path intermediateDir;
    @Getter
    private final Path outputDir;

    public Coordinator(List<String> inputFiles, int numReduceTasks) {
        this.inputFiles = new ArrayList<>(inputFiles);
        this.numReduceTasks = numReduceTasks;
        this.totalMapTasks = this.inputFiles.size();
        this.pendingMapInputFiles = new LinkedList<>(this.inputFiles);

        this.intermediateDir = Paths.get(intermediateDirName);
        this.outputDir = Paths.get(outputDirName);

        try {
            deleteDirectory(intermediateDir);
            deleteDirectory(outputDir);
            Files.createDirectories(intermediateDir);
            Files.createDirectories(outputDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create directories", e);
        }
    }

    public Task getTask(int workerId) {
        synchronized (lock) {
            if (!allMapTasksReportedDone) {
                if (!pendingMapInputFiles.isEmpty()) {
                    String inputFile = pendingMapInputFiles.poll();
                    int mapTaskId = nextMapTaskId.getAndIncrement();
                    log.info("Coordinator: Worker {} is assigned Map task ID={} for file: {}", workerId, mapTaskId, inputFile);
                    return new MapTask(inputFile, mapTaskId, numReduceTasks, this, workerId);
                } else {
                    if (completedMapTaskIds.size() < totalMapTasks) {
                        log.info("Coordinator: Waiting for Map tasks to complete {} / {} ...", completedMapTaskIds.size(), totalMapTasks);
                        try {
                            lock.wait(500);
                        } catch (InterruptedException e) { Thread.currentThread().interrupt(); return new ShutdownTask(); }
                        return new NoTaskAvailable();
                    } else {
                        allMapTasksReportedDone = true;
                        log.info("Coordinator: All Map tasks completed. Preparing Reduce tasks.");
                    }
                }
            }

            if (allMapTasksReportedDone && !reduceTasksPrepared) {
                prepareReduceTasks();
                reduceTasksPrepared = true;
                log.info("Coordinator: Reduce tasks prepared.");
                lock.notifyAll();
            }

            if (reduceTasksPrepared && !allReduceTasksReportedDone) {
                if (!pendingReduceTaskIds.isEmpty()) {
                    int reduceTaskId = pendingReduceTaskIds.poll();
                    List<String> filesForReduce = collectIntermediateFilesForReduceId(reduceTaskId);
                    log.info("Coordinator: Worker {} is assigned Reduce task ID={} with {} files.",
                            workerId, reduceTaskId, filesForReduce.size());
                    return new ReduceTask(reduceTaskId, filesForReduce, workerId, this);
                } else {
                    if (completedReduceTaskIds.size() < numReduceTasks) {
                        log.info("Coordinator: Waiting for Reduce tasks to complete {} / {} ...",
                                completedReduceTaskIds.size(), numReduceTasks);
                        try {
                            lock.wait(500);
                        } catch (InterruptedException e) { Thread.currentThread().interrupt(); return new ShutdownTask(); }
                        return new NoTaskAvailable();
                    } else {
                        allReduceTasksReportedDone = true;
                        log.info("Coordinator: All Reduce tasks completed.");
                        lock.notifyAll();
                    }
                }
            }

            if (allMapTasksReportedDone && allReduceTasksReportedDone) {
                log.info("Coordinator: Worker {} is shutting down.", workerId);
                return new ShutdownTask();
            }

            return new NoTaskAvailable();
        }
    }

    private void prepareReduceTasks() {
        for (int i = 0; i < numReduceTasks; i++) {
            pendingReduceTaskIds.add(i);
        }
    }

    private List<String> collectIntermediateFilesForReduceId(int reduceTaskId) {
        List<String> filesForThisReduceTask = new ArrayList<>();
        for (List<String> intermediateFilesFromMapTask : mapTaskOutputFiles.values()) {
            for (String filePathStr : intermediateFilesFromMapTask) {
                Path filePath = Paths.get(filePathStr);
                String fileName = filePath.getFileName().toString();
                String[] parts = fileName.split("-");
                if (parts.length == 3) {
                    try {
                        int fileReduceId = Integer.parseInt(parts[2]);
                        if (fileReduceId == reduceTaskId) {
                            filesForThisReduceTask.add(filePathStr);
                        }
                    } catch (NumberFormatException e) {
                        log.error("Coordinator: Error parsing file name {}", fileName, e);
                    }
                }
            }
        }
        return filesForThisReduceTask;
    }

    public void mapTaskCompleted(int workerId, int mapTaskId, List<String> createdIntermediateFiles) {
        synchronized (lock) {
            if (completedMapTaskIds.contains(mapTaskId)) {
                log.info("Coordinator: Worker {} re-reported map task {}. Ignoring.", workerId, mapTaskId);
                return;
            }
            completedMapTaskIds.add(mapTaskId);
            mapTaskOutputFiles.put(mapTaskId, createdIntermediateFiles);
            log.info("Coordinator: Worker {} completed map task {}. Intermediate files: {}. Total Map completed: {}/{}.",
                    workerId, mapTaskId, createdIntermediateFiles, completedMapTaskIds.size(), totalMapTasks);
            if (completedMapTaskIds.size() == totalMapTasks) {
                allMapTasksReportedDone = true;
                log.info("Coordinator: ALL Map tasks completed.");
            }
            lock.notifyAll();
        }
    }

    public void reduceTaskCompleted(int workerId, int reduceTaskId, String finalOutputFile) {
        synchronized (lock) {
            if (completedReduceTaskIds.contains(reduceTaskId)) {
                log.info("Coordinator Worker {} re-reported reduce task {}. Ignoring.", workerId, reduceTaskId);
                return;
            }
            completedReduceTaskIds.add(reduceTaskId);
            log.info("Coordinator: Worker {} completed reduce task {}. Final output file: {}. Total Reduce completed: {}/{}.",
                    workerId, reduceTaskId, finalOutputFile, completedReduceTaskIds.size(), numReduceTasks);
            if (completedReduceTaskIds.size() == numReduceTasks) {
                allReduceTasksReportedDone = true;
                log.info("Coordinator: ALL Reduce tasks completed.");
            }
            lock.notifyAll();
        }
    }

    public boolean isDone() {
        synchronized (lock) {
            return allMapTasksReportedDone && allReduceTasksReportedDone &&
                    completedMapTaskIds.size() == totalMapTasks &&
                    completedReduceTaskIds.size() == numReduceTasks;
        }
    }

    public void waitUntilAllDone() {
        synchronized (lock) {
            while (!isDone()) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Coordinator interrupted while waiting for completion.", e);
                    break;
                }
            }
        }
        log.info("Coordinator: All MapReduce work completed.");
    }

    private void deleteDirectory(Path directoryPath) throws IOException {
        if (Files.exists(directoryPath)) {
            Files.walk(directoryPath)
                    .sorted(java.util.Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            log.error("Failed to delete " + path, e);
                        }
                    });
        }
    }
}
