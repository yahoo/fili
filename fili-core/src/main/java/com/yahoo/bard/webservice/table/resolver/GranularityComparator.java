// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.table.PhysicalTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;

/**
 * Comparator to prefer coarser physical tables (i.e. fewer rows).
 */
public class GranularityComparator implements Comparator<PhysicalTable> {

    private static final Logger LOG = LoggerFactory.getLogger(GranularityComparator.class);
    private static final GranularityComparator COMPARATOR = new GranularityComparator();

    /**
     * A constructor provided in case other classes need to subclass GranularityComparator.
     */
    protected GranularityComparator() {
        // This constructor is intentionally empty. Nothing special is needed here.
    }

    /**
     * Factory method for creating new GranularityComparator instance.
     *
     * @return a new GranularityComparator instance
     */
    public static GranularityComparator getInstance() {
        return COMPARATOR;
    }

    /**
     * Compare two physical tables identifying which one has fewer time buckets.
     *
     * @param table1 The first table
     * @param table2 The second table
     *
     * @return negative if table1 has coarser grain (i.e. fewer rows per time) than table2
     */
    @Override
    public int compare(final PhysicalTable table1, final PhysicalTable table2) {
        // compare to returns -1 if the timeGrain for table1 is finer (expressed in more milliseconds) than table2
        int compare = table1.getSchema().getTimeGrain()
                .getEstimatedDuration()
                .compareTo(table2.getSchema().getTimeGrain().getEstimatedDuration());
        LOG.trace("{} {} {}", table1, compare < 0 ? "<" : ">", table2);
        // shorter duration means more rows per time, so negate to order by fewer rows rather than shorter duration
        return -1 * compare;
    }
}
