package by.lobanov.learntocodejavacore.classes.nested;

public class ClassWithNestedClass {

    public void method() {
        // nested class
        class LocalClass {
            void print() {
                System.out.println("Local class");
            }
        }
        new LocalClass().print();
    }
}
