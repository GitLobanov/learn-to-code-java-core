package by.lobanov.learntocodejavacore.concurrency.mapreduce.function.impl;

import by.lobanov.learntocodejavacore.concurrency.mapreduce.function.*;
import by.lobanov.learntocodejavacore.concurrency.mapreduce.model.*;

import java.io.*;
import java.nio.charset.*;
import java.nio.file.*;
import java.util.*;

/**
 * WordCountMapFunction - realization of MapFunction interface,
 * which is used to map words from a file to a list of KeyValue objects.
 */
public class WordCountMapFunction implements MapFunction {

    /**
     * Разделяем content на слова и для каждого слова создаем объект KeyVal
     * где key - это слово, а value - это "1". И возвращаем список этих объектов
     *
     * @param filename
     * @return {@link KeyValue}
     */
    @Override
    public List<KeyValue> map(String filename) {

        List<KeyValue> results = new ArrayList<>();
        String contents;

        try {
            contents = Files.readString(Path.of(filename), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        StringTokenizer tokenizer = new StringTokenizer(contents, " \t\n\r\f.,:;?!\"'-()");
        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken().toLowerCase();
            if (!word.isEmpty()) {
                results.add(new KeyValue(word, "1"));
            }
        }
        return results;
    }
}
