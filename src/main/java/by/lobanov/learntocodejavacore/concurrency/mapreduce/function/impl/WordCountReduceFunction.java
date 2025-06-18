package by.lobanov.learntocodejavacore.concurrency.mapreduce.function.impl;

import by.lobanov.learntocodejavacore.concurrency.mapreduce.function.*;

import java.util.*;

/**
 * Reducer function that counts the number of occurrences of each word.
 */
public class WordCountReduceFunction implements ReduceFunction {

    @Override
    public String reduce(String key, List<String> values) {
        return String.valueOf(values.size());
    }
}