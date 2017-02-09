// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.data.PartialDataHandler;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.util.IntervalUtils;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import java.util.Collections;
import java.util.Comparator;

/**
 * Comparator to prefer less partial data duration within the query.
 */
public class PartialTimeComparator implements Comparator<PhysicalTable> {

    private final PartialDataHandler partialDataHandler;
    private final QueryPlanningConstraint requestConstraints;

    /**
     * Constructor.
     *
     * @param requestConstraints contains the request constraints extracted from DataApiRequest and TemplateDruidQuery
     * @param handler  Handler for Partial Data
     */
    public PartialTimeComparator(QueryPlanningConstraint requestConstraints, PartialDataHandler handler) {
        this.requestConstraints = requestConstraints;
        this.partialDataHandler = handler;
    }

    /**
     * Compare two Physical Tables based on how much missing time they have.
     *
     * @param left The first table
     * @param right The second table
     *
     * @return negative if table1 has less missing time than table2
     */
    @Override
    public int compare(PhysicalTable left, PhysicalTable right) {
        // choose table with most data available for given columns
        long missingDurationLeft = IntervalUtils.getTotalDuration(
                partialDataHandler.findMissingTimeGrainIntervals(
                        requestConstraints.getAllColumnNames(),
                        Collections.singleton(left),
                        new SimplifiedIntervalList(requestConstraints.getIntervals()),
                        requestConstraints.getRequestGranularity()
                )
        );
        long missingDurationRight = IntervalUtils.getTotalDuration(
                partialDataHandler.findMissingTimeGrainIntervals(
                        requestConstraints.getAllColumnNames(),
                        Collections.singleton(right),
                        new SimplifiedIntervalList(requestConstraints.getIntervals()),
                        requestConstraints.getRequestGranularity()
                )
        );
        long difference = missingDurationLeft - missingDurationRight;
        return (int) Math.max(Math.min(Integer.MAX_VALUE, difference), Integer.MIN_VALUE);
    }
}
