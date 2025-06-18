package by.lobanov.learntocodejavacore.concurrency.mapreduce.task.impl;

import by.lobanov.learntocodejavacore.concurrency.mapreduce.task.*;
import org.slf4j.*;

/**
 * If no task is available, this task is returned.
 */
public class NoTaskAvailable implements Task {

    Logger log = LoggerFactory.getLogger(NoTaskAvailable.class);

    @Override
    public TaskType getType() {
        return Task.TaskType.NO_TASK_AVAILABLE;
    }

    /**
     * If not have tasks to do, worker will rest a little
     *
     * @param workerId
     * @throws InterruptedException
     */
    public void execute(int workerId) {
        log.info("Not have tasks. Worker {} is resting", workerId);
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "NoTaskAvailable";
    }
}
