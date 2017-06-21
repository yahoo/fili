// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.evaluator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * Utility class for dynamically dispatching calls.
 */
public class DispatchUtils {
    /**
     * Private constructor - all methods static.
     */
    private DispatchUtils() {

    }

    /**
     * Calls the current (overloaded) method in the callers class with
     * the given parameter types and provided parameters.
     *
     * @param parameterClasses  The classes of the parameters for the method being invoked.
     * @param parameters  The objects to be passed in as the parameters to the method.
     * @param <E>  The return type of the method.
     *
     * @return the evaluated value.
     */
    public static <E> E dispatch(Class caller, String methodName, Class[] parameterClasses, Object... parameters) {
        try {
            Method toInvoke = caller.getDeclaredMethod(methodName, parameterClasses);
            toInvoke.setAccessible(true);
            return (E) toInvoke.invoke(null, parameters);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new UnsupportedOperationException(
                    "Can't " + caller.getSimpleName() + "." + methodName + Arrays.toString(parameters),
                    e
            );
        }
    }
}
