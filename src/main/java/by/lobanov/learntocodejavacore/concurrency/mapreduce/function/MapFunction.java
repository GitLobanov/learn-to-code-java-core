package by.lobanov.learntocodejavacore.concurrency.mapreduce.function;

import by.lobanov.learntocodejavacore.concurrency.mapreduce.model.*;

import java.util.*;

@FunctionalInterface
public interface MapFunction {
    List<KeyValue> map(String filename);
}
