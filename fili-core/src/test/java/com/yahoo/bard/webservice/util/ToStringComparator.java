// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Compare two objects by their toString values. This ensures a consistent order, but not a particularly useful one.
 * @param <T> the type of objects that may be compared by this comparator
 */
public class ToStringComparator<T> implements Comparator<T>, Serializable {
    @Override
    public int compare(final T t1, final T t2) {
        return t1.toString().compareTo(t2.toString());
    }
}
