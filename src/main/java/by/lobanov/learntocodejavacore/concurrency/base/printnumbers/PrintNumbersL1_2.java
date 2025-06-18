package by.lobanov.learntocodejavacore.concurrency.base.printnumbers;

import java.util.concurrent.atomic.*;

public class PrintNumbersL1_2 {

    private static final AtomicBoolean isEvenTurn = new AtomicBoolean(true);
    private static final int TIMES_PRINT_PER_THREAD = 10;

    public static void main(String[] args) throws InterruptedException {
        PrintNumbersL1_2 p12 = new PrintNumbersL1_2();
        Thread evenThread = new Thread(p12.evenTask);
        Thread oddThread = new Thread(p12.oddTask);

        evenThread.start();
        oddThread.start();

        evenThread.join();
        oddThread.join();
    }

    private Runnable evenTask = () -> {
        for (int i = 0; i < TIMES_PRINT_PER_THREAD; i = i + 2) {
            while (!isEvenTurn.get()) {
                // Busy-waiting (плохой подход, но работает)
            }
            System.out.println("Even: " + i);
            isEvenTurn.set(false);
        }
    };

    private Runnable oddTask = () -> {
        for (int i = 1; i < TIMES_PRINT_PER_THREAD; i = i + 2) {
            while (isEvenTurn.get()) {
                // Busy-waiting (плохой подход, но работает)
            }
            System.out.println("Odd: " + i);
            isEvenTurn.set(true);
        }
    };
}
