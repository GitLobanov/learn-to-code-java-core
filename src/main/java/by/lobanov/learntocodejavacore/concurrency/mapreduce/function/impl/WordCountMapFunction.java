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
     * Split content into words and create a KeyValue object for each word
     * where key is the word and value is "1". Return a list of these objects
     *
     * @param filename, contents - content of the file with the given
     * @return {@link KeyValue}
     */
    @Override
    public List<KeyValue> map(String filename, String contents) {
        if (!Files.exists(Paths.get(filename))) {
            throw new RuntimeException("File not found: " + filename);
        }

        List<KeyValue> results = new ArrayList<>();

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
