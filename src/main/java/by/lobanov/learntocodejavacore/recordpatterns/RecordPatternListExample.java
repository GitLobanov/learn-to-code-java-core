package by.lobanov.learntocodejavacore.recordpatterns;

import java.util.*;

public class RecordPatternListExample {

    record Order(String id, List<Item> items) {}
    record Item(String name, double price) {}

    static void processOrder(Object obj) {
        if (obj instanceof Order(String id, List<Item> items)) {
            System.out.println("Order ID: " + id);
            for (var item : items) {
                if (item instanceof Item(String name, double price)) {
                    System.out.println("Item: " + name + ", Price: " + price);
                }
            }
        }
    }

    public static void main(String[] args) {
        List<Item> listItems = new ArrayList<>();
        listItems.add(new Item("Lighter", 300));
        listItems.add(new Item("Flashlight", 400));

        Order order = new Order(UUID.randomUUID().toString(), listItems);

        processOrder(order);
    }
}
