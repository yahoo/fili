// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import org.joda.time.Interval;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Compare intervals based on their ending instant.
 * <p>
 * Note: this comparator imposes orderings that are inconsistent with equals. Only the ending instant is considered.
 */
public class IntervalEndComparator implements Comparator<Interval>, Serializable {

    public static final IntervalEndComparator INSTANCE = new IntervalEndComparator();

    @Override
    public int compare(Interval first, Interval seoncd) {
        return first.getEnd().compareTo(seoncd.getEnd());
    }
}
