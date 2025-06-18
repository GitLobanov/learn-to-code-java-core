package by.lobanov.learntocodejavacore.concurrency.mapreduce;

import by.lobanov.learntocodejavacore.concurrency.mapreduce.function.*;
import by.lobanov.learntocodejavacore.concurrency.mapreduce.task.*;
import by.lobanov.learntocodejavacore.concurrency.mapreduce.task.impl.*;
import org.slf4j.*;

public class Worker implements Runnable {

    private final Logger log = LoggerFactory.getLogger(Worker.class);

    private final int workerId;
    private final Coordinator coordinator;
    private final MapFunction mapFunction;
    private final ReduceFunction reduceFunction;

    public Worker(int workerId, Coordinator coordinator, MapFunction mapFunction, ReduceFunction reduceFunction) {
        this.workerId = workerId;
        this.coordinator = coordinator;
        this.mapFunction = mapFunction;
        this.reduceFunction = reduceFunction;
    }

    @Override
    public void run() {
        log.info("Worker {} started.", workerId);
        try {
            while (true) {
                Task task = coordinator.getTask(workerId);

                switch (task) {
                    case MapTask mapTask -> mapTask.execute(mapFunction);
                    case ReduceTask reduceTask -> reduceTask.execute(reduceFunction);
                    case ShutdownTask ignored -> {
                        log.info("Worker {} received SHUTDOWN.", workerId);
                        return;
                    }
                    case NoTaskAvailable noTaskAvailable -> noTaskAvailable.execute(workerId);
                    default -> log.info("Worker {} received an unknown task {}", workerId, task);
                }
            }
        } catch (Exception e) {
            log.error("Worker {} encountered a critical error", workerId, e);
        }
        log.info("Worker {} finished.", workerId);
    }
}
