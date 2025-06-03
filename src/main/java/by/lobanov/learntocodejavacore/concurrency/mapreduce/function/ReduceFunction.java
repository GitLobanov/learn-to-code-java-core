package by.lobanov.learntocodejavacore.concurrency.mapreduce.function;

import java.util.*;

@FunctionalInterface
public interface ReduceFunction {
    String reduce(String key, List<String> values);
}