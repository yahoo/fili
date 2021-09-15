// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import com.yahoo.bard.webservice.table.PhysicalTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * A comparator which accepts a list of other comparators to apply in order until an imbalance is found.
 *
 * @param <T>  Type of objects that may be compared by this comparator
 */
public class ChainingComparator<T> implements Comparator<T> {

    private static final Logger LOG = LoggerFactory.getLogger(ChainingComparator.class);

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

        if (true) { // MLM
            return debuggingCompare(o1, o2);
        }
        return comparators.stream()
                .mapToInt(comparator -> comparator.compare(o1, o2))
                .filter(v -> v != 0)
                .findFirst()
                .orElse(0);
    }

    public int debuggingCompare(T o1, T o2) {
        final List<String> comparatorValues = new ArrayList<>();
        final AtomicReference<Comparator> winner = new AtomicReference<>();
        PhysicalTable pt1 = (PhysicalTable) o1;
        PhysicalTable pt2 = (PhysicalTable) o2;

        int result = comparators.stream()
//                .peek(c -> {
//                    if (c.compare(o1, o2) != 0 && winner.get() == null) { winner.set(c); }
//                })
                .peek(c -> comparatorValues.add(" " + c.getClass() + " " + (c.compare(o1, o2))))
                .mapToInt(comparator -> comparator.compare(o1, o2))
                .filter(v -> v != 0)
                .findFirst()
                .orElse(0);
        String comparatorResults = comparatorValues.stream().collect(Collectors.joining());
        LOG.debug("MLM: Matching comparator: " + comparatorResults + " values o1 "
                + pt1.getClass() + " "
                + pt1.getDataSourceNames() + " "
                + pt1.getAvailableIntervals() + " "
                + ", o2: "
                + pt2.getClass() + " "
                + pt2.getDataSourceNames() + " "
                + pt2.getAvailableIntervals() + " "
                + " value: " + result);
        return result;
    }
}
