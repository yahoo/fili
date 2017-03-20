// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.table.PhysicalTable;

import org.joda.time.Interval;

import java.util.Collection;

/**
 * Test that the start of each interval in a list falls on a time bucket of the table under test.
 */
public class IsTableStartAlignedWithIntervals implements IsTableAligned {

    private final Collection<Interval> alignsTo;

    /**
     * Constructor.
     *
     * @param alignsTo  Collection of intervals to determine alignment with when testing.
     */
    public IsTableStartAlignedWithIntervals(Collection<Interval> alignsTo) {
        this.alignsTo = alignsTo;
    }

    /**
     * Test whether table aligns with the interval collection supplied.
     *
     * @param table  The table whose alignment is under test
     *
     * @return true if this physical table's buckets can align to the request.
     */
    @Override
    public boolean test(PhysicalTable table) {
        return alignsTo.stream().allMatch(table.getSchema().getTimeGrain()::aligns);
    }
}
