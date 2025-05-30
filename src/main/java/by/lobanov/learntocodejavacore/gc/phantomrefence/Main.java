package by.lobanov.learntocodejavacore.gc.phantomrefence;

import java.lang.ref.*;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        Thread.sleep(10000);

        ReferenceQueue<TestClass> queue = new ReferenceQueue<>();
        TestClass test = new TestClass();
        Reference ref = new MyPhantomReference<>(test, queue);

        System.out.println("ref = " + ref);

        Thread.sleep(5000);

        System.out.println("Вызывается сборка мусора!");

        System.gc();
        Thread.sleep(300);

        System.out.println("ref = " + ref);

        Thread.sleep(5000);

        System.out.println("Вызывается сборка мусора!");

        System.gc();
    }
}
