package by.lobanov.learntocodejavacore.annotation.apectj;

import org.aspectj.lang.*;
import org.aspectj.lang.annotation.*;
import org.springframework.stereotype.*;

import java.lang.reflect.*;
import java.util.*;

@Aspect
@Component
public class MnemosyneAspect {

    @Pointcut("execution(public String *.toString()) && @within(by.lobanov.learntocodejavacore.annotation.apectj.MnemosyneNarrate)")
    public void narratedClassToStringExecution() {}

    @Around("narratedClassToStringExecution()")
    public Object generateCustomToString(ProceedingJoinPoint pjp) throws Throwable {
        Object target = pjp.getTarget(); // Экземпляр объекта, чей toString() вызывается

        if (target == null) {
            return "null"; // Или pjp.proceed() если хотим стандартное поведение для null
        }

        Class<?> targetClass = target.getClass();
        MnemosyneNarrate narrateAnnotation = targetClass.getAnnotation(MnemosyneNarrate.class);

        if (narrateAnnotation == null) {
            return pjp.proceed();
        }

        StringJoiner joiner = new StringJoiner(", ",
                narrateAnnotation.includeClassName() ? targetClass.getSimpleName() + "{" : "{",
                "}");

        Set<String> includes = new HashSet<>(Arrays.asList(narrateAnnotation.includeFields()));
        Set<String> excludes = new HashSet<>(Arrays.asList(narrateAnnotation.excludeFields()));

        Field[] fields = targetClass.getDeclaredFields();

        for (Field field : fields) {
            if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                continue;
            }

            String fieldName = field.getName();

            if (!excludes.isEmpty() && excludes.contains(fieldName)) {
                continue;
            }

            if (!includes.isEmpty() && !includes.contains(fieldName)) {
                continue;
            }

            try {
                field.setAccessible(true);
                Object value = field.get(target);
                String valueStr;

                if (value == null) {
                    valueStr = "null";
                } else if (value.getClass().isArray()) {
                    if (value instanceof Object[]) valueStr = Arrays.deepToString((Object[]) value);
                    else if (value instanceof int[]) valueStr = Arrays.toString((int[]) value);
                    else if (value instanceof long[]) valueStr = Arrays.toString((long[]) value);
                    else if (value instanceof double[]) valueStr = Arrays.toString((double[]) value);
                    else if (value instanceof float[]) valueStr = Arrays.toString((float[]) value);
                    else if (value instanceof boolean[]) valueStr = Arrays.toString((boolean[]) value);
                    else if (value instanceof byte[]) valueStr = Arrays.toString((byte[]) value);
                    else if (value instanceof char[]) valueStr = Arrays.toString((char[]) value);
                    else if (value instanceof short[]) valueStr = Arrays.toString((short[]) value);
                    else valueStr = value.toString();
                } else {
                    // ВАЖНО: Предотвращение рекурсии для toString() того же объекта
                    if (value == target) {
                        valueStr = "(this Collection)"; // или аналогичный маркер для рекурсии
                    } else {
                        // Здесь мы вызываем toString() для дочернего объекта.
                        // Если он тоже аннотирован @MnemosyneNarrate, аспект сработает и для него.
                        // Для сложных циклических графов все равно нужны продвинутые техники (ThreadLocal и т.д.)
                        valueStr = value.toString();
                    }
                }

                if (value instanceof String) {
                    joiner.add(fieldName + "='" + valueStr + "'");
                } else {
                    joiner.add(fieldName + "=" + valueStr);
                }

            } catch (IllegalAccessException e) {
                joiner.add(fieldName + "=<inaccessible>");
            } catch (StackOverflowError soe) {
                // Попытка отловить рекурсию, если простой проверки `value == target` не хватило
                joiner.add(fieldName + "=<recursion detected>");
            }
        }
        return joiner.toString();
    }
}