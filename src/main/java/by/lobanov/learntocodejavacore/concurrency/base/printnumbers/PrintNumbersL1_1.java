package by.lobanov.learntocodejavacore.concurrency.base.printnumbers;

public class PrintNumbersL1_1 {

    private static final Object monitor = new Object();
    private static final int TIMES_PRINT_PER_THREAD = 10;
    private static boolean available = false;

    public static void main(String[] args) throws InterruptedException {
        PrintNumbersL1_1 p11 = new PrintNumbersL1_1();
        Thread evenThread = new Thread(p11.evenTask);
        Thread oddThread = new Thread(p11.oddTask);
        oddThread.start();
        evenThread.start();

        evenThread.join();
        oddThread.join();
    }

    private Runnable evenTask= () -> {
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
    };

    private Runnable oddTask = () -> {
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
    };
}
