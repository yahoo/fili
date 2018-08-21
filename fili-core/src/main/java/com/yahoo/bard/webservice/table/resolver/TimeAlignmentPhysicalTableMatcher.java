// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.table.availability.IsTableAligned;
import com.yahoo.bard.webservice.table.availability.IsTableStartAlignedWithIntervals;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Tests whether a physical table aligns with the request time and period.
 */
public class TimeAlignmentPhysicalTableMatcher implements PhysicalTableMatcher {

    public static final Logger LOG = LoggerFactory.getLogger(TimeAlignmentPhysicalTableMatcher.class);

    public static final ErrorMessageFormat MESSAGE_FORMAT = ErrorMessageFormat.TABLE_ALIGNMENT_UNDEFINED;

    private final IsTableAligned isTableAligned;
    private final String logicalTableName;
    private final List<Interval> requestIntervals;

    /**
     * Stores the request table name and intervals and creates a predicate to test a physical table based on request
     * intervals.
     *
     * @param requestConstraint  Contains the request constraints extracted from DataApiRequest and TemplateDruidQuery
     */
    public TimeAlignmentPhysicalTableMatcher(QueryPlanningConstraint requestConstraint) {
        if (requestConstraint.getIntervals().isEmpty()) {
            throw new IllegalStateException("Intervals cannot be empty");
        }
        logicalTableName = requestConstraint.getLogicalTable().getName();
        requestIntervals = requestConstraint.getIntervals();
        isTableAligned = new IsTableStartAlignedWithIntervals(requestConstraint.getIntervals());
    }

    @Override
    public boolean test(PhysicalTable table) {
        return isTableAligned.test(table);
    }

    @Override
    public NoMatchFoundException noneFoundException() {
        LOG.error(MESSAGE_FORMAT.logFormat(logicalTableName, requestIntervals));
        return new NoMatchFoundException(MESSAGE_FORMAT.format(logicalTableName, requestIntervals));
    }
}
