package by.lobanov.learntocodejavacore.stream.mapmulti;

import by.lobanov.learntocodejavacore.recordpatterns.*;

import java.util.*;
import java.util.stream.*;

public class StreamMapMultiOrders {

    record Order(String id, List<Item> items) {}
    record Item(String name, double price) {}

    public static void main(String[] args) {
        List<Item> listItems = new ArrayList<>();
        listItems.add(new Item("Lighter", 300));
        listItems.add(new Item("Flashlight", 400));

        Order order = new Order(UUID.randomUUID().toString(), listItems);

        Stream.of(order)
                .mapMulti((o, downstream) -> {
                    downstream.accept(o.items());
                })
                .forEach(System.out::print);

        System.out.println("\n"+"-".repeat(10));

        Stream.of(order)
                .flatMap(o -> {
                    return o.items.stream();
                })
                .forEach(System.out::print);
    }
}
