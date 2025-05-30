package by.lobanov.learntocodejavacore.annotation.reflectionApi.onlyJsonIgnore;

import lombok.*;

import java.io.*;
import java.math.BigDecimal;
import java.util.UUID;

public class TestMain {

    @SneakyThrows
    public static void main(String[] args) {

        var trStats = new TransactionStatistic (
                UUID.randomUUID().toString(),
                BigDecimal.valueOf(100000L),
                BigDecimal.valueOf(10000L),
                BigDecimal.valueOf(10000L),
                "Auto",
                UUID.randomUUID().toString());

        FileOutputStream fileOut = new FileOutputStream("transaction_stats.json");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(trStats);
        out.close();
        fileOut.close();

        FileInputStream fileIn = new FileInputStream("transaction_stats.json");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        var stats = (TransactionStatistic) in.readObject();
        in.close();
        fileIn.close();

        System.out.println(stats.toString());
    }
}
