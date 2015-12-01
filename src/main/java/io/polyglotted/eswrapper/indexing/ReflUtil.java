package io.polyglotted.eswrapper.indexing;

import java.lang.reflect.Field;

abstract class ReflUtil {

    public static <T> T fieldValue(T object, String fieldName, Object value) {
        Field field = declaredField(object.getClass(), fieldName);
        try {
            field.setAccessible(true);
            field.set(object, value);
            return object;
        } catch (Exception e) {
            throw new IllegalStateException("unable to set field value for " + field, e);
        }
    }

    static Field declaredField(Class<?> clazz, String name) {
        Field result = null;
        while (clazz != Object.class) {
            try {
                result = clazz.getDeclaredField(name);
                break;
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        return result;
    }
}
