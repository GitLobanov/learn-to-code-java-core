package by.lobanov.learntocodejavacore.annotation.reflectionApi.moreJsonAnnotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(value = RetentionPolicy.RUNTIME)
@Target(value = ElementType.FIELD)
public @interface JsonMasked {

    TypeMasked value() default TypeMasked.HALF;

    enum TypeMasked {
        HALF, ALL
    }
}
