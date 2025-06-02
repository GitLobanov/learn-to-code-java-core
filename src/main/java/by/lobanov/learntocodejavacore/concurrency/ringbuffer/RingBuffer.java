package by.lobanov.learntocodejavacore.concurrency.ringbuffer;

public interface RingBuffer <T> {

    boolean offer(T t) throws InterruptedException;
    T take() throws InterruptedException;
    boolean isEmpty();
    boolean isFull();
    int size();
}
