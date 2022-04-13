package com.github.kputnam.hadoop.template.util;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Created by kputnam on 5/9/14.
 */
public class MagicUtil {

    public static Class<?> getTypeParam(Class<?> instantiatedType, int k) {
        Type genericType = instantiatedType.getGenericSuperclass();

        if (!(genericType instanceof ParameterizedType))
            throw new IllegalArgumentException("Parent class is not generic: " + genericType);

        Type[] typeParams = ((ParameterizedType) genericType).getActualTypeArguments();

        if (typeParams.length < k - 1)
            throw new IllegalArgumentException("No such argument (" + k + "): " + genericType);

        if (!(typeParams[k] instanceof Class<?>))
            throw new IllegalArgumentException("Type parameter was not instantiated: " + typeParams[k]);

        return (Class<?>) typeParams[k];
    }
}
