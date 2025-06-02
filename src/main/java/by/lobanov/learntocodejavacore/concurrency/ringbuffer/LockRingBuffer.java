package by.lobanov.learntocodejavacore.concurrency.ringbuffer;

import java.util.concurrent.locks.*;

public class LockRingBuffer<T> implements RingBuffer<T> {

    private final T[] buffer;
    private final int capacity;
    private int count;
    private int readPointer;
    private int writePointer;

    private final Lock lock = new ReentrantLock();

    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();

    public LockRingBuffer(int capacity) {
        this.capacity = capacity;
        buffer = (T[]) new Object[capacity];
        readPointer = 0;
        writePointer = 0;
        count = 0;
    }

    @Override
    public boolean offer(T item) throws InterruptedException {
        lock.lock();
        try {
            while (count == capacity) {
                notFull.await();
            }
            buffer[writePointer] = item;
            writePointer = (writePointer + 1) % capacity;
            count++;

            notEmpty.signal();
        } finally {
            lock.unlock();
        }
        return true;
    }

    @Override
    public T take() throws InterruptedException {
        lock.lock();
        try {
            while (count == 0) {
                notEmpty.await();
            }
            T item = buffer[readPointer];
            readPointer = (readPointer + 1) % capacity;
            count--;

            notFull.signal();
            return item;
        }  finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        return count == 0;
    }

    @Override
    public boolean isFull() {
        return count == capacity;
    }

    @Override
    public int size() {
        return count;
    }
}
