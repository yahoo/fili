// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import java.util.Locale;
import java.util.Map;

/**
 * Helper methods for enums.
 */
public class EnumUtils {
    /**
     * Boilerplate method to retrieve an enum method via an arbitrary key, rather than just the enum value.
     *
     * @param <T> Enum we are getting the value by key for
     * @param key  Key to get the enum value for
     * @param mapping  Mapping to use to look up the value by key
     * @param enumeration  Enum we are getting value by key for (needed due to type erasure)
     *
     * @return The enum value for the key
     * @throws IllegalArgumentException if this enum type has no constant with the specified name
     */
    public static <T extends Enum<T>> T forKey(String key, Map<String, T> mapping, Class<T> enumeration) {
        T t = mapping.get(key);
        if (t != null) {
            return t;
        }
        throw new IllegalArgumentException("Not an alternate key for " + enumeration.toString() + ": " + key);
    }

    /**
     * Boilerplate method to retrieve an enum method via an arbitrary key, rather than just the enum value.
     *
     * @param <T> Enum we are getting the value by key for
     * @param key  Key to get the enum value for
     * @param mapping  Mapping to use to look up the value by key
     * @param enumeration  Enum we are getting value by key for (needed due to type erasure)
     *
     * @return The enum value for the key
     * @throws IllegalArgumentException if this enum type has no constant with the specified name
     */
    public static <T extends Enum<T>> T forKey(int key, Map<Integer, T> mapping, Class<T> enumeration) {
        T t = mapping.get(key);
        if (t != null) {
            return t;
        }
        throw new IllegalArgumentException("Not an alternate key for " + enumeration.toString() + ": " + key);
    }

    /**
     * Converts the enum value to a camel case string. e.g ENUM_CONST_VALUE {@literal ->} enumConstValue
     *
     * @param e  enum
     *
     * @return enum string changed to camel case format
     */
    public static String enumJsonName(Enum<?> e) {
        return camelCase(e.name());
    }

    /**
     * Converts the string value to a camel case string. e.g ENUM_CONST_VALUE {@literal ->} enumConstValue
     *
     * @param s  string
     *
     * @return enum string changed to camel case format
     */
    public static String camelCase(String s) {
        String[] words = s.toLowerCase(Locale.ENGLISH).split("_");
        StringBuilder lowerCamelCase = new StringBuilder(words[0]);

        for (int i = 1; i < words.length; i++) {
            lowerCamelCase.append((words[i].substring(0, 1)).toUpperCase(Locale.ENGLISH));
            lowerCamelCase.append(words[i].substring(1));
        }
        return lowerCamelCase.toString();
    }
}
