package by.lobanov.learntocodejavacore.concurrency.mapreduce;

import by.lobanov.learntocodejavacore.concurrency.mapreduce.function.*;
import by.lobanov.learntocodejavacore.concurrency.mapreduce.function.impl.*;
import org.slf4j.*;

import java.io.*;
import java.nio.charset.*;
import java.nio.file.*;
import java.util.*;

/**
 * MapReduceApplication is a simple implementation of the MapReduce programming model.
 * It processes a set of input files, applies a map function to each file, and then a reduce function
 * to aggregate the results. The application uses multiple worker threads to perform the map and reduce tasks.
 */
public class MapReduceApplication {

    private static Logger log = LoggerFactory.getLogger(MapReduceApplication.class);

    public static void main(String[] args) {
        int numWorkers = 3;
        int numReduceTasks = 2; // m
        List<String> inputFilenames = Arrays.asList("input1.txt", "input2.txt", "input3.txt");

        createTestInputFiles(inputFilenames);

        log.info("MapReduceApplication started. {} workers, {} reduce tasks.", numWorkers, numReduceTasks);

        MapFunction mapFunc = new WordCountMapFunction();
        ReduceFunction reduceFunc = new WordCountReduceFunction();
        Coordinator coordinator = new Coordinator(inputFilenames, numReduceTasks);

        List<Thread> workerThreads = new ArrayList<>();
        for (int i = 0; i < numWorkers; i++) {
            Worker worker = new Worker(i, coordinator, mapFunc, reduceFunc);
            Thread thread = new Thread(worker, "Worker-" + i);
            workerThreads.add(thread);
            thread.start();
        }

        long startTime = System.currentTimeMillis();
        coordinator.waitUntilAllDone();
        long endTime = System.currentTimeMillis();

        log.info("MapReduce completed. Total time: {} ms.", (endTime - startTime));
        log.info("Intermediate files: {}", coordinator.getIntermediateDir().toAbsolutePath());

        for (Thread thread : workerThreads) {
            try {
                thread.join(5000);
                if (thread.isAlive()) {
                    log.error("Worker {} did not finish in time, interrupting...", thread.getName());
                    thread.interrupt();
                    thread.join(1000);
                }
            } catch (InterruptedException e) {
                log.error("Main thread was interrupted while waiting for worker {} to finish.", thread.getName());
                Thread.currentThread().interrupt();
            }
        }
        log.info("All worker threads should be finished.");
    }

    private static void createTestInputFiles(List<String> filenames) {
        try {
            String[] contents = {
                    "hello world java mapreduce",
                    "world is big java is fun",
                    "mapreduce example hello fun"
            };
            for (int i = 0; i < filenames.size(); i++) {
                Path filePath = Paths.get(filenames.get(i));
                String content = (i < contents.length) ? contents[i] : "default content for " + filenames.get(i);
                Files.writeString(filePath, content, StandardCharsets.UTF_8);
            }
            log.info("Test input files created.");
        } catch (IOException e) {
            log.error("Error creating test input files: {}", e.getMessage());
        }
    }

}
