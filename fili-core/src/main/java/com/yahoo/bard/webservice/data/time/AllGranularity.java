// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.time;

import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;

import org.joda.time.Interval;

import java.util.Collection;
import java.util.Iterator;

/**
 * All granularity buckets any number and length of intervals into a single bucket.
 * <p>
 * Because it has special behavior compared with period based time grains it gets it's own type.
 */
public class AllGranularity implements Granularity {

    public static final String ALL_NAME = "all";

    public static final AllGranularity INSTANCE = new AllGranularity();

    /**
     * Constructor.
     */
    private AllGranularity() {
    }

    @JsonValue
    public String getName() {
        return ALL_NAME;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object that) {
        return that instanceof AllGranularity;
    }

    @Override
    public Iterator<Interval> intervalsIterator(SimplifiedIntervalList intervals) {
        return intervals.iterator();
    }

    /**
     * Any granularity can be rolled up into all.
     *
     * @param that  The granularity to be compared against
     *
     * @return true
     */
    @Override
    public boolean satisfiedBy(Granularity that) {
        return true;
    }

    @Override
    public boolean accepts(Collection<Interval> intervals) {
        return true;
    }

    @Override
    @JsonIgnore
    public String getAlignmentDescription() {
        return "";
    }
}
