package by.lobanov.learntocodejavacore.concurrency.base.printnumbers;

import java.util.concurrent.*;

public class PrintNumbersL3_1 {

    private static int counter = 0;
    private static final int TIMES_PRINT_PER_THREAD = 10;
    private static final Semaphore evenSemaphore = new Semaphore(1);
    private static final Semaphore oddSemaphore = new Semaphore(0);

    public static void main(String[] args) throws InterruptedException {
        evenThread.start();
        oddThread.start();

        evenThread.join();
        oddThread.join();
    }

    private static Thread evenThread = new Thread(() -> {
        for (int i = 0; i < TIMES_PRINT_PER_THREAD; i++) {
            try {
                evenSemaphore.acquire();
                printNumber();
                oddSemaphore.release();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    });

    private static Thread oddThread = new Thread(() -> {
        for (int i = 1; i < TIMES_PRINT_PER_THREAD; i++) {
            try {
                oddSemaphore.acquire();
                printNumber();
                evenSemaphore.release();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    });

    private static void printNumber() {
        System.out.println(String.format("%s-%d", Thread.currentThread(), counter++));
    }
}
