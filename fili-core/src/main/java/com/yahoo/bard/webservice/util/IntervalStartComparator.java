// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import org.joda.time.Interval;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Compare intervals based on their starting instant.
 * <p>
 * Note: this comparator imposes orderings that are inconsistent with equals. Only the starting instant is considered.
 */
public class IntervalStartComparator implements Comparator<Interval>, Serializable {

    public static final IntervalStartComparator INSTANCE = new IntervalStartComparator();

    @Override
    public int compare(Interval a, Interval b) {
        return a.getStart().compareTo(b.getStart());
    }
}
