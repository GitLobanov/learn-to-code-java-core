package by.lobanov.learntocodejavacore.reflection.logic;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class AppleTree extends Tree{

    private List<Fruit> fruits;

    public AppleTree() {
        setName("AppleTree");
        fruits = new ArrayList<>();

        for (int i = 0; i < new Random().nextInt(0, 10); i++) {
            fruits.add(new Fruit("Apple №" + i));
        }
    }

    public List<Fruit> getFruits() {
        return fruits;
    }

    public void addFruit(Fruit fruit) {
        this.fruits.add(fruit);
    }

    public void addBatchFruits(List<Fruit> fruits) {
        this.fruits.addAll(fruits);
    }
}
