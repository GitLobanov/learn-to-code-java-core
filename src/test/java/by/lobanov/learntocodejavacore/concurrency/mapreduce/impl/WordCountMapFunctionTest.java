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
        String content = "Hello, parameterized test!";
        Files.writeString(testFile, content);

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString(), content);

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
        String content = "hello world hello";
        Files.writeString(testFile, content);

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString(), content);

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
        String content = "Hello, world! How are you?";
        Files.writeString(testFile, content);

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString(), content);

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
    void testMapCasesInsensitive() throws IOException {
        // given
        Path testFile = tempDir.resolve("cases.txt");
        String content = "Hello HELLO hello HeLLo";
        Files.writeString(testFile, content);

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString(), content);

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
        String content = "";
        Files.writeString(testFile, content);

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString(), content);

        // then
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testMapWhitespaceOnly() throws IOException {
        // given
        Path testFile = tempDir.resolve("whitespace.txt");
        String content = "   \t\n\r\f   ";
        Files.writeString(testFile, content);

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString(), content);

        // then
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testMapPunctuationOnly() throws IOException {
        // given
        Path testFile = tempDir.resolve("punctuation.txt");
        String content = ".,;:!?\"'-()";
        Files.writeString(testFile, content);

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString(), content);

        // then
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testMapWithNumbers() throws IOException {
        // given
        Path testFile = tempDir.resolve("numbers.txt");
        String content = "test 123 hello 456";
        Files.writeString(testFile, content);

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString(), content);

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
        String content = "first line\nsecond line\nthird line";
        Files.writeString(testFile, content);

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString(), content);

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
        String content = "The quick brown fox jumps over the lazy dog. The dog was really lazy!";
        Files.writeString(testFile, content);

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString(), content);

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
        String content = "vladimir! hello-world test_case";
        Files.writeString(testFile, content);

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString(), content);

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
        String content = "some content";

        // when & then
        assertThrows(RuntimeException.class, () -> {
            wordCountMapFunction.map(nonExistentFile, content);
        });
    }

    @Test
    void testMapAllValuesAreOne() throws IOException {
        // given
        Path testFile = tempDir.resolve("values.txt");
        String content = "different words have different meanings but same count";
        Files.writeString(testFile, content);

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString(), content);

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
        String content = "word1\t\tword2   word3\r\nword4";
        Files.writeString(testFile, content);

        // when
        List<KeyValue> result = wordCountMapFunction.map(testFile.toString(), content);

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
