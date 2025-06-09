package by.lobanov.learntocodejavacore.concurrency.base.printnumbers;

import java.util.concurrent.locks.*;

public class PrintNumbersL2_1 {

    private static int counter = 0;
    private static final int TIMES_PRINT_PER_THREAD = 10;

    private static final Lock lock = new ReentrantLock();
    private static final Condition oddCondition = lock.newCondition();
    private static final Condition evenCondition = lock.newCondition();
    private static boolean isEvenTurn = true;


    public static void main(String[] args) throws InterruptedException {
        PrintNumbersL2_1 p21 = new PrintNumbersL2_1();
        Thread evenThread = new Thread(p21.evenThread);
        Thread oddThread = new Thread(p21.oddThread);

        evenThread.start();
        oddThread.start();

        evenThread.join();
        oddThread.join();
    }

    private Runnable evenThread = () -> {
        for (int i = 0; i < TIMES_PRINT_PER_THREAD; i++) {
            try {
                lock.lock();
                while (!isEvenTurn) {
                    evenCondition.await();
                }
                printNumber();
                isEvenTurn = false;
                oddCondition.signal();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
        }
    };

    private Runnable oddThread = () -> {
        for (int i = 1; i < TIMES_PRINT_PER_THREAD; i++) {
            try {
                lock.lock();
                while (isEvenTurn) {
                    oddCondition.await();
                }
                printNumber();
                isEvenTurn = true;
                evenCondition.signal();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
        }
    };

    private static void printNumber() {
        System.out.println(String.format("%s-%d", Thread.currentThread(), counter++));
    }
}
