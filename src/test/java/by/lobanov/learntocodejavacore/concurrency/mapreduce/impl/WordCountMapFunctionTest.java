package by.lobanov.learntocodejavacore.concurrency.mapreduce.impl;

import by.lobanov.learntocodejavacore.concurrency.mapreduce.function.impl.*;
import by.lobanov.learntocodejavacore.concurrency.mapreduce.model.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class WordCountMapFunctionTest {

    @TempDir
    Path tempDir;

    WordCountMapFunction wordCountMapFunction = new WordCountMapFunction();

    @Test
    void testMap() throws IOException {
        // given
        Path testFile = tempDir.resolve("test.txt");
        Files.writeString(testFile, "Hello, parameterized test!");

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString());

        // then
        assertNotNull(result);
        assertEquals(3, result.size());

        List<String> words = result.stream().map(kv -> kv.key).toList();
        assertTrue(words.contains("hello"));
        assertTrue(words.contains("parameterized"));
        assertTrue(words.contains("test"));

        // Verify all values are "1"
        assertTrue(result.stream().allMatch(kv -> "1".equals(kv.value)));
    }

    @Test
    void testMapSimpleText() throws IOException {
        // given
        Path testFile = tempDir.resolve("simple.txt");
        Files.writeString(testFile, "hello world hello");

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString());

        // then
        assertNotNull(result);
        assertEquals(3, result.size());

        List<String> words = result.stream().map(kv -> kv.key).toList();
        assertTrue(words.contains("hello"));
        assertTrue(words.contains("world"));

        long helloCount = result.stream().filter(kv -> "hello".equals(kv.key)).count();
        assertEquals(2, helloCount);
    }

    @Test
    void testMapWithPunctuation() throws IOException {
        // given
        Path testFile = tempDir.resolve("punctuation.txt");
        Files.writeString(testFile, "Hello, world! How are you?");

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString());

        // then
        assertNotNull(result);
        assertEquals(5, result.size());

        List<String> words = result.stream().map(kv -> kv.key).toList();
        assertTrue(words.contains("hello"));
        assertTrue(words.contains("world"));
        assertTrue(words.contains("how"));
        assertTrue(words.contains("are"));
        assertTrue(words.contains("you"));

        // Verify all values are "1"
        assertTrue(result.stream().allMatch(kv -> "1".equals(kv.value)));
    }

    @Test
    void testMapCaseInsensitive() throws IOException {
        // given
        Path testFile = tempDir.resolve("case.txt");
        Files.writeString(testFile, "Hello HELLO hello HeLLo");

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString());

        // then
        assertNotNull(result);
        assertEquals(4, result.size());

        // All should be converted to lowercase
        assertTrue(result.stream().allMatch(kv -> "hello".equals(kv.key)));
    }

    @Test
    void testMapEmptyFile() throws IOException {
        // given
        Path testFile = tempDir.resolve("empty.txt");
        Files.writeString(testFile, "");

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString());

        // then
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testMapWhitespaceOnly() throws IOException {
        // given
        Path testFile = tempDir.resolve("whitespace.txt");
        Files.writeString(testFile, "   \t\n\r\f   ");

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString());

        // then
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testMapPunctuationOnly() throws IOException {
        // given
        Path testFile = tempDir.resolve("punctuation.txt");
        Files.writeString(testFile, ".,;:!?\"'-()");

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString());

        // then
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testMapWithNumbers() throws IOException {
        // given
        Path testFile = tempDir.resolve("numbers.txt");
        Files.writeString(testFile, "test 123 hello 456");

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString());

        // then
        assertNotNull(result);
        assertEquals(4, result.size());

        List<String> words = result.stream().map(kv -> kv.key).toList();
        assertTrue(words.contains("test"));
        assertTrue(words.contains("123"));
        assertTrue(words.contains("hello"));
        assertTrue(words.contains("456"));
    }

    @Test
    void testMapMultilineText() throws IOException {
        // given
        Path testFile = tempDir.resolve("multiline.txt");
        Files.writeString(testFile, "first line\nsecond line\nthird line");

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString());

        // then
        assertNotNull(result);
        assertEquals(6, result.size());

        List<String> words = result.stream().map(kv -> kv.key).toList();
        assertTrue(words.contains("first"));
        assertTrue(words.contains("second"));
        assertTrue(words.contains("third"));
        assertTrue(words.contains("line"));

        long lineCount = result.stream().filter(kv -> "line".equals(kv.key)).count();
        assertEquals(3, lineCount);
    }

    @Test
    void testMapComplexText() throws IOException {
        // given
        Path testFile = tempDir.resolve("complex.txt");
        Files.writeString(testFile, "The quick brown fox jumps over the lazy dog. The dog was really lazy!");

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString());

        // then
        assertNotNull(result);
        assertEquals(14, result.size());

        // Count specific words
        long theCount = result.stream().filter(kv -> "the".equals(kv.key)).count();
        long dogCount = result.stream().filter(kv -> "dog".equals(kv.key)).count();
        long lazyCount = result.stream().filter(kv -> "lazy".equals(kv.key)).count();

        assertEquals(3, theCount);
        assertEquals(2, dogCount);
        assertEquals(2, lazyCount);
    }

    @Test
    void testMapWithSpecialCharacters() throws IOException {
        // given
        Path testFile = tempDir.resolve("special.txt");
        Files.writeString(testFile, "vladimir! hello-world test_case");

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString());

        // then
        assertNotNull(result);
        assertEquals(4, result.size());

        List<String> words = result.stream().map(kv -> kv.key).toList();
        assertTrue(words.contains("vladimir"));
        assertTrue(words.contains("hello"));
        assertTrue(words.contains("world"));
        assertTrue(words.contains("test_case"));
    }

    @Test
    void testMapFileNotFound() {
        // given
        String nonExistentFile = tempDir.resolve("nonexistent.txt").toString();

        // when & then
        assertThrows(RuntimeException.class, () -> {
            wordCountMapFunction.map(nonExistentFile);
        });
    }

    @Test
    void testMapAllValuesAreOne() throws IOException {
        // given
        Path testFile = tempDir.resolve("values.txt");
        Files.writeString(testFile, "different words have different meanings but same count");

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString());

        // then
        assertNotNull(result);
        assertFalse(result.isEmpty());

        // All values should be "1"
        assertTrue(result.stream().allMatch(kv -> "1".equals(kv.value)));
    }

    @Test
    void testMapWithTabsAndMultipleSpaces() throws IOException {
        // given
        Path testFile = tempDir.resolve("tabs.txt");
        Files.writeString(testFile, "word1\t\tword2   word3\r\nword4");

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString());

        // then
        assertNotNull(result);
        assertEquals(4, result.size());

        List<String> words = result.stream().map(kv -> kv.key).toList();
        assertTrue(words.contains("word1"));
        assertTrue(words.contains("word2"));
        assertTrue(words.contains("word3"));
        assertTrue(words.contains("word4"));
    }
}
