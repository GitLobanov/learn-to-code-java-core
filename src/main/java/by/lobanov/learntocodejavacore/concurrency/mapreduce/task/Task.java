package by.lobanov.learntocodejavacore.concurrency.mapreduce.task;

public interface Task {
    enum TaskType {
        MAP,
        REDUCE,
        SHUTDOWN,
        NO_TASK_AVAILABLE // Задача "нет доступных задач"
    }
    TaskType getType();
}
