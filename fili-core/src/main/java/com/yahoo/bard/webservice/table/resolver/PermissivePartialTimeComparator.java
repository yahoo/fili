// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.data.PartialDataHandler;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.util.IntervalUtils;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Comparator to prefer less partial data duration within the query.
 */
public class PermissivePartialTimeComparator extends PartialTimeComparator {

    private static final Logger LOG = LoggerFactory.getLogger(PermissivePartialTimeComparator.class);

    /**
     * Constructor.
     *
     * @param requestConstraint contains the request constraints extracted from DataApiRequest and TemplateDruidQuery
     * @param handler  Handler for Partial Data
     */
    public PermissivePartialTimeComparator(QueryPlanningConstraint requestConstraint, PartialDataHandler handler) {
        super(requestConstraint, handler);
    }

    /**
     * Compare two Physical Tables based on how much missing time they have.  Explicitly ignores request constraints.
     *
     * @param left The first table
     * @param right The second table
     *
     * @return negative if table1 has less missing time than table2
     */
    @Override
    public int compare(PhysicalTable left, PhysicalTable right) {
        // choose table with most data available for given columns

        SimplifiedIntervalList leftAvailable = left.getAvailableIntervals();
        long missingDurationLeft = IntervalUtils.getTotalDuration(
                partialDataHandler.findMissingTimeGrainIntervals(
                        left.getName(),
                        leftAvailable.intersect(requestedIntervals),
                        new SimplifiedIntervalList(requestConstraint.getIntervals()),
                        requestConstraint.getRequestGranularity()
                )
        );
        SimplifiedIntervalList rightAvailable = right.getAvailableIntervals();
        long missingDurationRight = IntervalUtils.getTotalDuration(
                partialDataHandler.findMissingTimeGrainIntervals(
                        right.getName(),
                        rightAvailable.intersect(requestedIntervals),
                        new SimplifiedIntervalList(requestConstraint.getIntervals()),
                        requestConstraint.getRequestGranularity()
                )
        );
        long difference = missingDurationLeft - missingDurationRight;
        int result = (int) Math.max(Math.min(Integer.MAX_VALUE, difference), Integer.MIN_VALUE);
        LOG.debug(
                "MLM: Comparing left {}, with available {} with missing {} and right {} " +
                        "with available {} with missing {}, result {}",
                left.getName(),
                leftAvailable,
                missingDurationLeft,
                right.getName(),
                rightAvailable,
                missingDurationRight,
                result
        );
        return result;
    }
}
