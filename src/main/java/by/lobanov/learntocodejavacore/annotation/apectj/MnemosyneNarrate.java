package by.lobanov.learntocodejavacore.annotation.apectj;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface MnemosyneNarrate {
    String[] includeFields() default {};
    String[] excludeFields() default {};
    boolean includeClassName() default true;
}