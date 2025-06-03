package by.lobanov.learntocodejavacore.concurrency.mapreduce.task.impl;

import by.lobanov.learntocodejavacore.concurrency.mapreduce.task.*;

/**
 * If you want to stop the worker, you can use this task.
 */
public class ShutdownTask implements Task {

    @Override
    public TaskType getType() {
        return Task.TaskType.SHUTDOWN;
    }

    @Override
    public String toString() {
        return "ShutdownTask";
    }
}
