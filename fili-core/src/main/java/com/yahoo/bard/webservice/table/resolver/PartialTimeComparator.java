// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.data.PartialDataHandler;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.util.IntervalUtils;
import com.yahoo.bard.webservice.web.DataApiRequest;

import java.util.Collections;
import java.util.Comparator;

/**
 * Comparator to prefer less partial data duration within the query
 */
public class PartialTimeComparator implements Comparator<PhysicalTable> {

    private final PartialDataHandler partialDataHandler;
    private final DataApiRequest request;
    private final TemplateDruidQuery query;

    public PartialTimeComparator(DataApiRequest request, TemplateDruidQuery query, PartialDataHandler handler) {
        this.request = request;
        this.query = query;
        this.partialDataHandler = handler;
    }

    /**
     * @param left The first table
     * @param right The second table
     *
     * @return negative if table1 has less missing time than table2
     */
    @Override
    public int compare(final PhysicalTable left, final PhysicalTable right) {
        // choose table with most data available for given columns
        long missingDurationLeft = IntervalUtils.getTotalDuration(
                partialDataHandler.findMissingRequestTimeGrainIntervals(request, query, Collections.singleton(left))
        );
        long missingDurationRight = IntervalUtils.getTotalDuration(
                partialDataHandler.findMissingRequestTimeGrainIntervals(
                        request,
                        query,
                        Collections.singleton(right)
                )
        );
        long difference = missingDurationLeft - missingDurationRight;
        return (int) Math.max(Math.min(Integer.MAX_VALUE, difference), Integer.MIN_VALUE);
    }
}
