package by.lobanov.learntocodejavacore.concurrency.ringbuffer;

public interface RingBuffer <T> {

    boolean offer(T t);
    T take();
    boolean isEmpty();
    boolean isFull();
    int size();

    void close();

    boolean isClosed();
}
