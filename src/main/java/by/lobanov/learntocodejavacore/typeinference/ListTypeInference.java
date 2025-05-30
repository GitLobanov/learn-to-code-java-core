package by.lobanov.learntocodejavacore.typeinference;

import java.util.*;

public class ListTypeInference {

    public static void main(String[] args) {
        List<Integer> list = new ArrayList<Integer>();
        List<Integer> list2 = new ArrayList<>();

        m(g());
    }

    static void m(Object o) {
        System.out.println("one");
    }

    static void m(String[] o) {
        System.out.println("two");
    }

    static <T> T g() {
        return null;
    }
}
