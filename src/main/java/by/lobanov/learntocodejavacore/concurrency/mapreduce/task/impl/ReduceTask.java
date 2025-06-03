package by.lobanov.learntocodejavacore.concurrency.mapreduce.task.impl;

import by.lobanov.learntocodejavacore.concurrency.mapreduce.*;
import by.lobanov.learntocodejavacore.concurrency.mapreduce.function.*;
import by.lobanov.learntocodejavacore.concurrency.mapreduce.model.*;
import by.lobanov.learntocodejavacore.concurrency.mapreduce.task.*;
import org.slf4j.*;

import java.io.*;
import java.nio.charset.*;
import java.nio.file.*;
import java.util.*;

/**
 * Task for performing the Reduce phase
 */
public class ReduceTask implements Task{

    public final int reduceTaskId;
    public final List<String> intermediateFilesToProcess;
    private final int workerId;

    private final Coordinator coordinator;

    Logger log = LoggerFactory.getLogger(MapTask.class);

    public ReduceTask(int reduceTaskId, List<String> intermediateFilesToProcess, int workerId, Coordinator coordinator) {
        this.reduceTaskId = reduceTaskId;
        this.intermediateFilesToProcess = intermediateFilesToProcess;
        this.workerId = workerId;
        this.coordinator = coordinator;
    }

    @Override
    public TaskType getType() {
        return TaskType.REDUCE;
    }

    /**
     * Execute the Reduce task using the provided reduce function.
     *
     * @param reduceFunction
     */
    public void execute(ReduceFunction reduceFunction) {
        log.info("Worker {} started Reduce task {}, with {} files", workerId, reduceTaskId, intermediateFilesToProcess.size());

        List<KeyValue> allKeyValuesFromFiles = getKeyValuesFromFiles();
        if (isKeyValueListEmpty(allKeyValuesFromFiles)) return;

        allKeyValuesFromFiles.sort(Comparator.comparing(kv -> kv.key));

        Path finalOutputFilePath = perfomeReduceAndReturnPath(reduceFunction, allKeyValuesFromFiles);
        coordinator.reduceTaskCompleted(workerId, reduceTaskId, finalOutputFilePath.toString());
    }

    /**
     * Read all key-value pairs from the intermediate files.
     *
     * @return
     */
    private List<KeyValue> getKeyValuesFromFiles() {
        List<KeyValue> allKeyValuesFromFiles = new ArrayList<>();

        for (String filePathStr : intermediateFilesToProcess) {
            Path filePath = Path.of(filePathStr);
            if (!Files.exists(filePath)) {
                log.error("Intermediate file not found: {} for Reduce task {}", filePathStr, reduceTaskId);
                continue;
            }
            try (BufferedReader reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t", 2);
                    if (parts.length == 2) {
                        allKeyValuesFromFiles.add(new KeyValue(parts[0], parts[1]));
                    } else {
                        System.err.println("Воркер " + workerId + ": Некорректная строка в файле " + filePathStr + ": '" + line + "'");
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return allKeyValuesFromFiles;
    }

    /**
     * Check if the list of key-value pairs is empty and handle accordingly.
     *
     * @param allKeyValuesFromFiles
     * @return
     */
    private boolean isKeyValueListEmpty(List<KeyValue> allKeyValuesFromFiles) {
        if (allKeyValuesFromFiles.isEmpty()) {
            log.warn("Worker {} has no data to process for Reduce task {}", workerId, reduceTaskId);
            String finalOutputFileName = String.format("mr-out-%d", reduceTaskId);
            Path finalOutputFilePath = coordinator.getOutputDir().resolve(finalOutputFileName);
            try {
                Files.createFile(finalOutputFilePath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            coordinator.reduceTaskCompleted(workerId, reduceTaskId, finalOutputFilePath.toString());
            return true;
        }
        return false;
    }

    /**
     * Perform the reduce operation and write the results to the final output file.
     *
     * @param reduceFunction
     * @param allKeyValuesFromFiles
     * @return
     */
    private Path perfomeReduceAndReturnPath(ReduceFunction reduceFunction, List<KeyValue> allKeyValuesFromFiles) {
        String finalOutputFileName = String.format("mr-out-%d", reduceTaskId);
        Path finalOutputFilePath = coordinator.getOutputDir().resolve(finalOutputFileName);

        try (BufferedWriter writer = Files.newBufferedWriter(finalOutputFilePath, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            int i = 0;
            while (i < allKeyValuesFromFiles.size()) {
                String currentKey = allKeyValuesFromFiles.get(i).key;
                List<String> valuesForCurrentKey = new ArrayList<>();
                int j = i;

                while (j < allKeyValuesFromFiles.size() && allKeyValuesFromFiles.get(j).key.equals(currentKey)) {
                    valuesForCurrentKey.add(allKeyValuesFromFiles.get(j).value);
                    j++;
                }

                String reducedResult = reduceFunction.reduce(currentKey, valuesForCurrentKey);
                writer.write(currentKey + " " + reducedResult);
                writer.newLine();

                i = j;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log.info("Worker {} completed Reduce task {}, final output file: {}", workerId, reduceTaskId, finalOutputFilePath);
        return finalOutputFilePath;
    }

    @Override
    public String toString() {
        return "ReduceTaskDetails{" +
                "reduceTaskId=" + reduceTaskId +
                ", intermediateFilesToProcess=" + intermediateFilesToProcess.size() + " files" +
                '}';
    }
}
