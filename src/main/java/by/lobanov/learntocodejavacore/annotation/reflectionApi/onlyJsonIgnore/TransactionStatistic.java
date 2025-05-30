package by.lobanov.learntocodejavacore.annotation.reflectionApi.onlyJsonIgnore;

import java.io.*;
import java.lang.reflect.*;
import java.math.BigDecimal;
import java.util.*;

public class TransactionStatistic implements Serializable {

    @JsonIgnore
    String clientId;
    BigDecimal totalIncomeInMonth;
    BigDecimal totalExpenseInMonth;
    BigDecimal totalSavingsInMonth;
    String popularCategoryInMonth;
    @JsonIgnore
    String requestId;

    public TransactionStatistic(String clientId, BigDecimal totalIncomeInMonth, BigDecimal totalExpenseInMonth,
                                BigDecimal totalSavingsInMonth, String popularCategoryInMonth, String requestId) {
        this.clientId = clientId;
        this.totalIncomeInMonth = totalIncomeInMonth;
        this.totalExpenseInMonth = totalExpenseInMonth;
        this.totalSavingsInMonth = totalSavingsInMonth;
        this.popularCategoryInMonth = popularCategoryInMonth;
        this.requestId = requestId;
    }


    @Override
    public String toString() {
        StringJoiner sj = new StringJoiner(", ", this.getClass().getSimpleName() + "{", "}");

        Field[] declaredFields = this.getClass().getDeclaredFields();

        for (Field field : declaredFields) {
            try {
                field.setAccessible(true);

                boolean isJsonIgnorePresent = field.isAnnotationPresent(JsonIgnore.class);
                Object value = field.get(this);

                if (isJsonIgnorePresent || value == null) {
                    continue;
                }

                String valueString;
                if (value instanceof String) {
                    valueString = "'" + value + "'";
                } else {
                    valueString = value.toString();
                }

                sj.add(field.getName() + "=" + valueString);

            } catch (IllegalAccessException e) {
                sj.add(field.getName() + "=<недоступно>");
            }
        }
        return sj.toString();
    }
}
