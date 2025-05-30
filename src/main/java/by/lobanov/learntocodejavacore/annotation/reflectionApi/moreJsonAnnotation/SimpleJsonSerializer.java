package by.lobanov.learntocodejavacore.annotation.reflectionApi.moreJsonAnnotation;

import by.lobanov.learntocodejavacore.annotation.reflectionApi.onlyJsonIgnore.*;

import java.lang.reflect.Field;
import java.util.StringJoiner;

public class SimpleJsonSerializer {

    public String serialize(Object object) {
        if (object == null) {
            return "null";
        }

        Class<?> clazz = object.getClass();
        StringJoiner jsonElements = new StringJoiner(",");

        try {
            for (Field field : clazz.getDeclaredFields()) {

                if (field.isAnnotationPresent(JsonIgnore.class)) {
                    continue;
                }

                field.setAccessible(true);

                String jsonFieldName;
                if (field.isAnnotationPresent(JsonField.class)) {
                    JsonField jsonFieldAnnotation = field.getAnnotation(JsonField.class);
                    String customName = jsonFieldAnnotation.value();
                    jsonFieldName = (customName != null && !customName.isEmpty()) ? customName : field.getName();
                } else {
                    jsonFieldName = field.getName();
                }

                Object value = field.get(object);
                String jsonValue;

                if (value instanceof String || value instanceof java.time.Instant) {
                    jsonValue = "\"" + value.toString() + "\"";
                } else if (value == null) {
                    jsonValue = "null";
                } else {
                    jsonValue = value.toString();
                }

                if (field.isAnnotationPresent(JsonMasked.class)) {
                    JsonMasked jsonFieldAnnotation = field.getAnnotation(JsonMasked.class);
                    JsonMasked.TypeMasked typeMasked = jsonFieldAnnotation.value();
                    Integer halfLength = jsonValue.length() / 2; // nu**
                    if (typeMasked.equals(JsonMasked.TypeMasked.HALF)) {
                        String regex = "\\S{"+halfLength+"}$";
                        jsonValue = jsonValue.replaceAll(regex, "****");
                    } else if (typeMasked.equals(JsonMasked.TypeMasked.ALL)) {
                        jsonValue = jsonValue.replaceAll("\\S", "*");
                    }
                }

                jsonElements.add("\"" + jsonFieldName + "\":" + jsonValue);
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Error accessing field during serialization", e);
        }

        return "{" + jsonElements.toString() + "}";
    }
}
