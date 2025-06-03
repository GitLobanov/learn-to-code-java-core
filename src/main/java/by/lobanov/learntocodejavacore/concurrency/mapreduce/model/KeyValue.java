package by.lobanov.learntocodejavacore.concurrency.mapreduce.model;

import lombok.*;

/**
 * Model class to represent a key-value pair.
 */
@Getter
public class KeyValue {

    public String key;
    public String value;

    public KeyValue(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return "KeyValue{" + "key='" + key + '\'' + ", value='" + value + '\'' + '}';
    }
}
