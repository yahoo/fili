// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.NO_TABLE_FOR_NON_AGGREGATABLE;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.util.StreamUtils;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Use the granularity and columns of a query to determine whether or not tables can satisfy this query.
 * <p>
 * If a given table contains non-agg dimensions, query must contain all these non-agg dimensions to use this table.
 */
public class AggregatableDimensionsMatcher implements PhysicalTableMatcher {

    public static final Logger LOG = LoggerFactory.getLogger(AggregatableDimensionsMatcher.class);

    public static final ErrorMessageFormat MESSAGE_FORMAT = NO_TABLE_FOR_NON_AGGREGATABLE;

    private final QueryPlanningConstraint requestConstraint;

    /**
     * Constructor saves metrics, dimensions, coarsest time grain, and logical table name (for logging).
     *
     * @param requestConstraint  Contains the request constraints extracted from DataApiRequest and TemplateDruidQuery
     */
    public AggregatableDimensionsMatcher(QueryPlanningConstraint requestConstraint) {
        this.requestConstraint = requestConstraint;
    }

    @Override
    public boolean test(PhysicalTable table) {
        Set<String> columnNames = requestConstraint.getAllColumnNames();

        // If table contains non-agg dimensions, query must contain all these non-agg dimensions to use this table.
        return table.getDimensions().stream()
                .filter(StreamUtils.not(Dimension::isAggregatable))
                .map(Dimension::getApiName)
                .allMatch(columnNames::contains);
    }

    @Override
    public NoMatchFoundException noneFoundException() {
        Set<String> aggDimensions = requestConstraint.getRequestDimensions().stream()
                .filter(Dimension::isAggregatable)
                .map(Dimension::getApiName)
                .collect(Collectors.toSet());

        Set<String> nonAggDimensions = requestConstraint.getRequestDimensions().stream()
                .filter(StreamUtils.not(Dimension::isAggregatable))
                .map(Dimension::getApiName)
                .collect(Collectors.toSet());

        LOG.error(MESSAGE_FORMAT.logFormat(nonAggDimensions, aggDimensions));
        return new NoMatchFoundException(MESSAGE_FORMAT.format(nonAggDimensions, aggDimensions));
    }
}
