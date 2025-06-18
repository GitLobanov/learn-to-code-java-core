package by.lobanov.learntocodejavacore.concurrency.mapreduce.impl;

import by.lobanov.learntocodejavacore.concurrency.mapreduce.function.impl.*;
import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WordCountReduceFunctionTest {

    WordCountReduceFunction wordCountReduceFunction = new WordCountReduceFunction();

    @Test
    void testReduceMultipleValues() {
        // given
        String key = "hello";
        List<String> values = Arrays.asList("1", "1", "1");

        // when
        String result = wordCountReduceFunction.reduce(key, values);

        // then
        assertEquals("3", result);
    }

    @Test
    void testReduceEmptyValues() {
        // given
        String key = "empty";
        List<String> values = new ArrayList<>();

        // when
        String result = wordCountReduceFunction.reduce(key, values);

        // then
        assertEquals("0", result);
    }

    @Test
    void testReduceLargeNumberOfValues() {
        // given
        String key = "frequent";
        List<String> values = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            values.add("1");
        }

        // when
        String result = wordCountReduceFunction.reduce(key, values);

        // then
        assertEquals("100", result);
    }

    @Test
    void testReduceWithDifferentKeys() {
        // given
        List<String> values = Arrays.asList("1", "1");

        // when
        String result1 = wordCountReduceFunction.reduce("word1", values);
        String result2 = wordCountReduceFunction.reduce("word2", values);
        String result3 = wordCountReduceFunction.reduce("", values);

        // then
        assertEquals("2", result1);
        assertEquals("2", result2);
        assertEquals("2", result3);
    }

    @Test
    void testReduceIgnoresValueContent() {
        // given - reduce function should only count values, not their content
        String key = "test";
        List<String> values = Arrays.asList("1", "2", "different", "anything");

        // when
        String result = wordCountReduceFunction.reduce(key, values);

        // then
        assertEquals("4", result);
    }

    @Test
    void testReduceWithNullKey() {
        // given
        String key = null;
        List<String> values = Arrays.asList("1", "1");

        // when
        String result = wordCountReduceFunction.reduce(key, values);

        // then
        assertEquals("2", result);
    }

    @Test
    void testReduceConsistency() {
        // given
        String key = "consistency";
        List<String> values1 = Arrays.asList("1", "1", "1", "1", "1");
        List<String> values2 = Arrays.asList("a", "b", "c", "d", "e");

        // when
        String result1 = wordCountReduceFunction.reduce(key, values1);
        String result2 = wordCountReduceFunction.reduce(key, values2);

        // then
        assertEquals("5", result1);
        assertEquals("5", result2);
        assertEquals(result1, result2);
    }
}
