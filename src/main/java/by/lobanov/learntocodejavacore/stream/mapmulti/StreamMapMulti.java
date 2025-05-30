package by.lobanov.learntocodejavacore.stream.mapmulti;

import java.util.stream.*;

public class StreamMapMulti {

    public static void main(String[] args) {
        Stream.of(1, 2, 3, 4)
                .mapMulti((number, downstream) -> {
                    if (number % 2 == 0) {
                        downstream.accept(number);
                        downstream.accept(number * 100);
                    }
                })
                .forEach(System.out::print);

        System.out.println("\n"+"-".repeat(10));

        Stream.of(1, 2, 3, 4)
                .flatMap(d -> {
                    if (d % 2 == 0) {
                        return Stream.of(d, d * 100);
                    }
                    return Stream.empty();
                })
                .forEach(System.out::print);
    }
}
