// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.evaluator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

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
     * Calls the given method in the caller's class with
     * the given parameter types and provided parameters.
     *
     * @return the evaluated value.
     */
    public static <T, R> R dispatch(Evaluator<T, R> evaluator, T parameter) {
        Class<?> caller = evaluator.getClass();
        try {
            Method toInvoke = caller.getDeclaredMethod("evaluate", Object.class);
            toInvoke.setAccessible(true);
            return (R) toInvoke.invoke(evaluator, parameter.getClass());
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new UnsupportedOperationException(
                    "Can't " + caller.getSimpleName() + ".evaluate(" + parameter + ")", e);
        } catch (InvocationTargetException e) {
            throw new UnsupportedOperationException(
                    "Can't " + caller.getSimpleName() + ".evaluate" + parameter + " because " +
                            e.getTargetException().getMessage(),
                    e.getTargetException()
            );
        }
    }
}
