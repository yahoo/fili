// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import java.util.Comparator;
import java.util.List;

/**
 * A comparator which accepts a list of other comparators to apply in order until an imbalance is found.
 *
 * @param <T>  Type of objects that may be compared by this comparator
 */
public class ChainingComparator<T> implements Comparator<T> {

    private final List<Comparator<T>> comparators;

    /**
     * Constructor.
     *
     * @param comparators  Comparators to chain
     */
    public ChainingComparator(List<Comparator<T>> comparators) {
        this.comparators = comparators;
    }

    @Override
    public int compare(T o1, T o2) {
        return comparators.stream()
                .mapToInt(comparator -> comparator.compare(o1, o2))
                .filter(result -> result != 0)
                .findFirst()
                .orElse(0);
    }
}
