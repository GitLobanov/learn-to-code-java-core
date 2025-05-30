package by.lobanov.learntocodejavacore.annotation.reflectionApi.moreJsonAnnotation;

import java.lang.annotation.*;

@Retention(value = RetentionPolicy.RUNTIME)
@Target(value = ElementType.FIELD)
public @interface JsonIgnore {
}
