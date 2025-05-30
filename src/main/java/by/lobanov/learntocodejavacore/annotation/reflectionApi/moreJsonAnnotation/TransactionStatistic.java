package by.lobanov.learntocodejavacore.annotation.reflectionApi.moreJsonAnnotation;

import lombok.*;

import java.math.*;

@Data
@Builder()
public class TransactionStatistic {

    String clientId;
    BigDecimal totalIncomeInMonth;
    BigDecimal totalExpenseInMonth;
    @JsonMasked(JsonMasked.TypeMasked.HALF)
    BigDecimal totalSavingsInMonth;
    String popularCategoryInMonth;
    @JsonIgnore
    String requestId;
}
