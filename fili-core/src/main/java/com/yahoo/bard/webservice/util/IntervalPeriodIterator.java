// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import org.apache.commons.lang3.ObjectUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.ReadablePeriod;

import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.validation.constraints.NotNull;

/**
 * An iterator that splits an interval into slices of length equal to a period and returns them
 * <p>
 * The slices returned are aligned to the interval start. Any partial slice at the end will return the remaining time.
 */
public class IntervalPeriodIterator implements Iterator<Interval> {

    private final ReadablePeriod period;
    private final DateTime intervalStart;
    private final DateTime intervalEnd;

    private int position;
    private DateTime currentPosition;

    /**
     * Constructor.
     *
     * @param period  The period to divide the interval by
     * @param baseInterval  The raw interval which is to be divided
     */
    public IntervalPeriodIterator(@NotNull ReadablePeriod period, Interval baseInterval) {
        this.period = period;
        intervalStart = baseInterval.getStart();
        intervalEnd = baseInterval.getEnd();
        position = 0;
        currentPosition = boundaryAt(0);

        // Chronology accepts null periods, we must not do so or this iterator is non-terminating
        if (period == null) {
            throw new IllegalArgumentException("Period cannot be null");
        }
    }

    @Override
    public boolean hasNext() {
        return currentPosition.isBefore(intervalEnd);
    }

    @Override
    public Interval next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        position += 1;
        DateTime nextPosition = ObjectUtils.min(intervalEnd, boundaryAt(position));
        Interval result = new Interval(currentPosition, nextPosition);
        currentPosition = nextPosition;
        return result;
    }

    /**
     * Find the start of a subinterval at a period based offset from the interval start.
     *
     * @param n  The number of periods from the start of the interval
     *
     * @return The calculated instant
     */
    private DateTime boundaryAt(int n) {
        long instant = intervalStart.getChronology().add(period, intervalStart.getMillis(), n);
        return new DateTime(instant);
    }
}
