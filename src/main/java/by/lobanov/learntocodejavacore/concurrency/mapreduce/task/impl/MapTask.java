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
 * Task for Map phase.
 */
public class MapTask implements Task {

    public final String inputFile;
    public final int mapTaskId;
    public final int numReduceTasks;
    private final int workerId;

    private final Coordinator coordinator;

    Logger log = LoggerFactory.getLogger(MapTask.class);

    public MapTask(String inputFile,
                   int mapTaskId,
                   int numReduceTasks,
                   Coordinator coordinator,
                   int workerId) {
        this.inputFile = inputFile;
        this.mapTaskId = mapTaskId;
        this.numReduceTasks = numReduceTasks;
        this.coordinator = coordinator;
        this.workerId = workerId;
    }

    @Override
    public TaskType getType() {
        return TaskType.MAP;
    }

    public void execute(MapFunction mapFunction) {
        log.info("Worker {} started Map task ID={} for file {}", workerId, mapTaskId, inputFile);
        var mappedKeyValues = mapFunction.map(inputFile);
        var buckets = mapKeyValuesToBuckets(mappedKeyValues);
        var intermediateFilesCreatedPaths = writeBucketsToFileAndReturnPaths(buckets);
        log.info("Worker {} finished Map task ID={} for file {}", workerId, mapTaskId, inputFile);
        coordinator.mapTaskCompleted(workerId, mapTaskId, intermediateFilesCreatedPaths);
    }

    /**
     * Map KeyValue to buckets.
     *
     * @param mappedKeyValues
     * @return map of buckets
     */
    private Map<Integer, List<KeyValue>> mapKeyValuesToBuckets(List<KeyValue> mappedKeyValues) {
        Map<Integer, List<KeyValue>> buckets = new HashMap<>();
        for (int i = 0; i < numReduceTasks; i++) {
            buckets.put(i, new ArrayList<>());
        }

        for (KeyValue kv : mappedKeyValues) {
            int reduceBucketIndex = Math.abs(kv.key.hashCode()) % numReduceTasks;
            buckets.get(reduceBucketIndex).add(kv);
        }
        return buckets;
    }

    /**
     * Write every bucket to a separate file.
     * Name of the file: mr-MapID-ReduceID (e.g., mr-0-1)
     *
     * @param buckets
     * @throws IOException
     */
    private List<String> writeBucketsToFileAndReturnPaths(Map<Integer, List<KeyValue>> buckets) {

        List<String> intermediateFilesCreatedPaths = new ArrayList<>();
        for (int i = 0; i < numReduceTasks; i++) {
            List<KeyValue> bucketContent = buckets.get(i);
            if (bucketContent.isEmpty()) {
                continue;
            }

            String intermediateFileName = String.format("mr-%d-%d", mapTaskId, i);
            Path intermediateFilePath = coordinator.getIntermediateDir().resolve(intermediateFileName);

            try (BufferedWriter writer = Files.newBufferedWriter(intermediateFilePath, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                for (KeyValue kv : bucketContent) {
                    writer.write(kv.key + "\t" + kv.value);
                    writer.newLine();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            intermediateFilesCreatedPaths.add(intermediateFilePath.toString());
        }

        return intermediateFilesCreatedPaths;
    }

    @Override
    public String toString() {
        return "MapTaskDetails{" +
                "inputFile='" + inputFile + '\'' +
                ", mapTaskId=" + mapTaskId +
                ", numReduceTasks=" + numReduceTasks +
                '}';
    }
}