package by.lobanov.learntocodejavacore.reflection.testcases;

import by.lobanov.learntocodejavacore.reflection.logic.AppleTree;
import by.lobanov.learntocodejavacore.reflection.logic.Tree;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class Main {

    public static void main(String[] args) {
        case1();
    }

    // перехватить название дерева и поменять его на 'Haha Oops'
    private static void case1 () {
        Tree tree = new AppleTree();
        try {
            Method setName = Class.forName("by.lobanov.learntocodejavacore.reflection.logic.Tree")
                    .getDeclaredMethod("setName", String.class);
            setName.setAccessible(true);
            setName.invoke(tree, "Haha Oops");
        } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        System.out.println(tree.getName());
    }
}
