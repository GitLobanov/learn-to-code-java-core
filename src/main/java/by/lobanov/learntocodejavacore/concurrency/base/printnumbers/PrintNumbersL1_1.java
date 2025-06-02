package by.lobanov.learntocodejavacore.concurrency.base.printnumbers;

public class PrintNumbersL1_1 {

    private static final Object monitor = new Object();
    private static final int TIMES_PRINT_PER_THREAD = 10;
    private static boolean available = false;

    public static void main(String[] args) throws InterruptedException {
        evenThread.start();
        oddThread.start();

        evenThread.join();
        oddThread.join();
    }

    private static Thread evenThread = new Thread(() -> {
        for (int i = 0; i < TIMES_PRINT_PER_THREAD; i = i + 2) {
            try {
                synchronized (monitor) {
                    while (available) {
                        monitor.wait();
                    }
                    System.out.println("Even: " + i);
                    available = true;
                    monitor.notify();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    });

    private static Thread oddThread = new Thread(() -> {
        for (int i = 1; i < TIMES_PRINT_PER_THREAD; i = i + 2) {
            try {
                synchronized (monitor) {
                    while (!available) {
                        monitor.wait();
                    }
                    System.out.println("Odd: " + i);
                    available = false;
                    monitor.notify();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    });
}
