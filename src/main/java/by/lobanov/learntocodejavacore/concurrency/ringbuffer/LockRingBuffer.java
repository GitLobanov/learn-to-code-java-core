package by.lobanov.learntocodejavacore.concurrency.ringbuffer;

import java.util.concurrent.locks.*;

public class LockRingBuffer<T> implements RingBuffer<T> {

    private final T[] buffer;
    private final int capacity;
    private int count;
    private int readPointer;
    private int writePointer;
    private boolean isClosed = false;

    private final Lock lock = new ReentrantLock();

    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();

    public LockRingBuffer(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be greater than 0");
        } else {
            this.capacity = capacity;
            buffer = (T[]) new Object[capacity];
            readPointer = 0;
            writePointer = 0;
            count = 0;
        }
    }

    @Override
    public boolean offer(T item) {
        if (item == null) {
            throw new NullPointerException("Cannot offer null item to the ring buffer");
        }

        lock.lock();
        try {
            while (count == capacity) {
                if (isClosed) {
                    throw new IllegalStateException("Cannot offer to a closed ring buffer");
                }

                try {
                    notFull.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
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
    public T take() {
        lock.lock();
        try {
            while (count == 0) {
                if (isClosed) {
                    throw new IllegalStateException("Cannot take from a closed ring buffer");
                }

                try {
                    notEmpty.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
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
        lock.lock();
        try {
            return count == 0;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isFull() {
        lock.lock();
        try {
            return count == capacity;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int size() {
        lock.lock();
        try {
            return count;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            isClosed = true;
            notFull.signalAll();
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isClosed() {
        lock.lock();
        try {
            return isClosed;
        } finally {
            lock.unlock();
        }
    }
}
