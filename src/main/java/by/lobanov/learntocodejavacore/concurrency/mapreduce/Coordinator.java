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

    private final Queue<String> pendingMapInputFiles;
    private final AtomicInteger nextMapTaskId = new AtomicInteger(0);
    private final Set<Integer> completedMapTaskIds = Collections.synchronizedSet(new HashSet<>());
    private final Set<Integer> failedMapTaskIds = Collections.synchronizedSet(new HashSet<>());
    private final Map<Integer, String> mapTaskIdToInputFile = new ConcurrentHashMap<>();
    private final int totalMapTasks;

    private final Map<Integer, List<String>> mapTaskOutputFiles = new ConcurrentHashMap<>();

    private final Queue<Integer> pendingReduceTaskIds = new LinkedList<>();
    private final Set<Integer> completedReduceTaskIds = Collections.synchronizedSet(new HashSet<>());
    private final Set<Integer> failedReduceTaskIds = Collections.synchronizedSet(new HashSet<>());

    private volatile boolean allMapTasksReportedDone = false;
    private volatile boolean reduceTasksPrepared = false;
    private volatile boolean allReduceTasksReportedDone = false;

    private final Object lock = new Object();

    private final String intermediateDirName = "intermediate_files";
    private final String outputDirName = "output_files";
    @Getter
    private final Path intermediateDir;
    @Getter
    private final Path outputDir;

    public Coordinator(List<String> inputFiles, int numReduceTasks) {
        if (inputFiles == null || inputFiles.isEmpty()) {
            throw new IllegalArgumentException("Input files cannot be null or empty");
        }
        if (numReduceTasks <= 0) {
            throw new IllegalArgumentException("Number of reduce tasks must be positive");
        }

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
            Task mapTask = tryGetMapTask(workerId);
            if (mapTask != null) {
                return mapTask;
            }

            prepareReduceTasksIfReady();

            Task reduceTask = tryGetReduceTask(workerId);
            if (reduceTask != null) {
                return reduceTask;
            }

            if (allMapTasksReportedDone && allReduceTasksReportedDone) {
                log.info("Coordinator: Worker {} is shutting down.", workerId);
                return new ShutdownTask();
            }

            return new NoTaskAvailable();
        }
    }

    private Task tryGetMapTask(int workerId) {
        while (!allMapTasksReportedDone) {
            if (!pendingMapInputFiles.isEmpty()) {
                return assignMapTask(workerId);
            }

            if (isAllMapTasksCompleted()) {
                allMapTasksReportedDone = true;
                log.info("Coordinator: All Map tasks completed. Preparing Reduce tasks.");
                break;
            }

            if (!waitForMapTasks(workerId)) {
                return new NoTaskAvailable();
            }
        }
        return null;
    }

    private Task assignMapTask(int workerId) {
        String inputFile = pendingMapInputFiles.poll();
        int mapTaskId = nextMapTaskId.getAndIncrement();
        mapTaskIdToInputFile.put(mapTaskId, inputFile);
        log.info("Coordinator: Worker {} is assigned Map task ID={} for file: {}", workerId, mapTaskId, inputFile);
        return new MapTask(inputFile, mapTaskId, numReduceTasks, this, workerId);
    }

    private boolean isAllMapTasksCompleted() {
        return completedMapTaskIds.size() >= totalMapTasks;
    }

    private boolean waitForMapTasks(int workerId) {
        int completedTasks = completedMapTaskIds.size();
        log.debug("Coordinator: Worker {} waiting for Map tasks, complete {} / {}",
                workerId, completedTasks, totalMapTasks);
        try {
            lock.wait();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Coordinator: Worker {} interrupted while waiting for Map tasks", workerId);
            return false;
        }
    }

    private void prepareReduceTasksIfReady() {
        if (allMapTasksReportedDone && !reduceTasksPrepared) {
            prepareReduceTasks();
            reduceTasksPrepared = true;
            log.info("Coordinator: Reduce tasks prepared.");
            lock.notifyAll();
        }
    }

    private Task tryGetReduceTask(int workerId) {
        while (reduceTasksPrepared && !allReduceTasksReportedDone) {
            if (!pendingReduceTaskIds.isEmpty()) {
                return assignReduceTask(workerId);
            }

            if (isAllReduceTasksCompleted()) {
                allReduceTasksReportedDone = true;
                log.info("Coordinator: All Reduce tasks completed.");
                lock.notifyAll();
                break;
            }

            if (!waitForReduceTasks(workerId)) {
                return new NoTaskAvailable(); // Прерван
            }
        }
        return null;
    }

    private Task assignReduceTask(int workerId) {
        int reduceTaskId = pendingReduceTaskIds.poll();
        List<String> filesForReduce = collectIntermediateFilesForReduceId(reduceTaskId);
        log.info("Coordinator: Worker {} is assigned Reduce task ID={} with {} files.",
                workerId, reduceTaskId, filesForReduce.size());
        return new ReduceTask(reduceTaskId, filesForReduce, workerId, this);
    }

    private boolean isAllReduceTasksCompleted() {
        return completedReduceTaskIds.size() >= numReduceTasks;
    }

    private boolean waitForReduceTasks(int workerId) {
        int completedTasks = completedReduceTaskIds.size();
        log.debug("Coordinator: Worker {} waiting for Reduce tasks, complete {} / {}",
                workerId, completedTasks, numReduceTasks);
        try {
            lock.wait();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Coordinator: Worker {} interrupted while waiting for Reduce tasks", workerId);
            return false;
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
        if (createdIntermediateFiles == null) {
            throw new IllegalArgumentException("Created intermediate files cannot be null");
        }

        synchronized (lock) {
            if (completedMapTaskIds.contains(mapTaskId)) {
                log.info("Coordinator: Worker {} re-reported map task {}. Ignoring.", workerId, mapTaskId);
                return;
            }
            completedMapTaskIds.add(mapTaskId);
            mapTaskOutputFiles.put(mapTaskId, Collections.unmodifiableList(createdIntermediateFiles));
            log.info("Coordinator: Worker {} completed map task {}. Intermediate files: {}. Total Map completed: {}/{}.",
                    workerId, mapTaskId, createdIntermediateFiles, completedMapTaskIds.size(), totalMapTasks);
            lock.notifyAll();
        }
    }

    public void mapTaskFailed(int workerId, int mapTaskId) {
        synchronized (lock) {
            if (completedMapTaskIds.contains(mapTaskId) || failedMapTaskIds.contains(mapTaskId)) {
                log.info("Coordinator: Worker {} re-reported failed map task {}. Ignoring.", workerId, mapTaskId);
                return;
            }
            failedMapTaskIds.add(mapTaskId);
            String inputFile = mapTaskIdToInputFile.get(mapTaskId);
            if (inputFile != null) {
                pendingMapInputFiles.add(inputFile);
                mapTaskIdToInputFile.remove(mapTaskId);
                log.info("Coordinator: Worker {} failed map task {}. Re-adding file {} to pending tasks.",
                        workerId, mapTaskId, inputFile);
            }
            lock.notifyAll();
        }
    }

    public void reduceTaskCompleted(int workerId, int reduceTaskId, String finalOutputFile) {
        synchronized (lock) {
            if (completedReduceTaskIds.contains(reduceTaskId)) {
                log.info("Coordinator Workers {} re-reported reduce task {}. Ignoring.", workerId, reduceTaskId);
                return;
            }
            completedReduceTaskIds.add(reduceTaskId);
            log.info("Coordinator: Worker {} completed reduce task {}. Final output file: {}. Total Reduce completed: {}/{}.",
                    workerId, reduceTaskId, finalOutputFile, completedReduceTaskIds.size(), numReduceTasks);

            // Check if all reduce tasks are completed and set the flag
            if (isAllReduceTasksCompleted()) {
                allReduceTasksReportedDone = true;
                log.info("Coordinator: All Reduce tasks completed.");
            }

            lock.notifyAll();
        }
    }

    public void reduceTaskFailed(int workerId, int reduceTaskId) {
        synchronized (lock) {
            if (completedReduceTaskIds.contains(reduceTaskId) || failedReduceTaskIds.contains(reduceTaskId)) {
                log.info("Coordinator: Worker {} re-reported failed reduce task {}. Ignoring.", workerId, reduceTaskId);
                return;
            }
            failedReduceTaskIds.add(reduceTaskId);
            log.info("Coordinator: Worker {} failed reduce task {}. Re-adding task {} to pending tasks.",
                    workerId, reduceTaskId, reduceTaskId);
            pendingReduceTaskIds.add(reduceTaskId);
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
            List<IOException> suppressedExceptions = new ArrayList<>();
            try (var stream = Files.walk(directoryPath)) {
                stream.sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                log.error("Failed to delete path: {}", path, e);
                                suppressedExceptions.add(e);
                            }
                        });
            }
            if (!suppressedExceptions.isEmpty()) {
                IOException mainException = new IOException("Failed to delete some files in directory: " + directoryPath);
                suppressedExceptions.forEach(mainException::addSuppressed);
                throw mainException;
            }
        }
    }

    public static class Builder {
        private List<String> inputFiles;
        private int numReduceTasks;

        public Builder inputFiles(List<String> inputFiles) {
            this.inputFiles = new ArrayList<>(inputFiles);
            return this;
        }

        public Builder numReduceTasks(int numReduceTasks) {
            this.numReduceTasks = numReduceTasks;
            return this;
        }

        public Coordinator build() {
            return new Coordinator(inputFiles, numReduceTasks);
        }
    }
}
