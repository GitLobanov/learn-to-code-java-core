package by.lobanov.learntocodejavacore.gc.phantomrefence;

import java.lang.ref.*;

public class MyPhantomReference<TestClass> extends PhantomReference<TestClass> {

    public MyPhantomReference(TestClass obj, ReferenceQueue<TestClass> queue) {

        super(obj, queue);

        Thread thread = new QueueReadingThread<TestClass>(queue);

        thread.start();
    }

    public void cleanup() {
        System.out.println("Очистка фантомной ссылки! Удаление объекта из памяти!");
        clear();
    }
}
