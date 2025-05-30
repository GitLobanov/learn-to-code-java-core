package by.lobanov.learntocodejavacore.annotation.reflectionApi.moreJsonAnnotation;

import java.math.*;
import java.util.*;

public class TestMain {

    public static void main(String[] args) {

        var trStats = TransactionStatistic.builder()
                .clientId(UUID.randomUUID().toString())
                .popularCategoryInMonth("Auto")
                .totalExpenseInMonth(BigDecimal.valueOf(100000L))
                .totalIncomeInMonth(BigDecimal.valueOf(10000L))
                .totalSavingsInMonth(BigDecimal.valueOf(10000L))
                .requestId(UUID.randomUUID().toString())
                .build();

        System.out.println("Original Object: " + trStats);
        SimpleJsonSerializer serializer = new SimpleJsonSerializer();
        String jsonOutput = serializer.serialize(trStats);
        System.out.println("Serialized JSON: " + jsonOutput);
    }
}
