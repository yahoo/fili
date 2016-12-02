// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.NO_TABLE_FOR_NON_AGGREGATABLE;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.util.StreamUtils;
import com.yahoo.bard.webservice.util.TableUtils;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Collectors;

/**
 *  Use the granularity and columns of a query to determine whether or not tables can satisfy this query.
 */
public class AggregatableDimensionsMatcher implements PhysicalTableMatcher {

    public static final Logger LOG = LoggerFactory.getLogger(AggregatableDimensionsMatcher.class);

    public static final ErrorMessageFormat MESSAGE_FORMAT = NO_TABLE_FOR_NON_AGGREGATABLE;

    private final DataApiRequest request;
    private final TemplateDruidQuery query;

    /**
     * Constructor saves metrics, dimensions, coarsest time grain, and logical table name (for logging).
     *
     * @param request  The request whose dimensions are being matched on
     * @param query  The query whose columns are being matched
     */
    public AggregatableDimensionsMatcher(DataApiRequest request, TemplateDruidQuery query) {
        this.request = request;
        this.query = query;
    }

    @Override
    public boolean test(PhysicalTable table) {
        Set<String> columnNames = TableUtils.getColumnNames(request, query.getInnermostQuery());

        return table.getDimensions().stream()
                .filter(StreamUtils.not(Dimension::isAggregatable))
                .map(Dimension::getApiName)
                .map(table::getPhysicalColumnName)
                .allMatch(columnNames::contains);
    }

    @Override
    public NoMatchFoundException noneFoundException() {
        Set<String> aggDimensions = request.getDimensions().stream()
                .filter(Dimension::isAggregatable)
                .map(Dimension::getApiName)
                .collect(Collectors.toSet());

        Set<String> nonAggDimensions = request.getDimensions().stream()
                .filter(StreamUtils.not(Dimension::isAggregatable))
                .map(Dimension::getApiName)
                .collect(Collectors.toSet());

        LOG.error(MESSAGE_FORMAT.logFormat(nonAggDimensions, aggDimensions));
        return new NoMatchFoundException(MESSAGE_FORMAT.format(nonAggDimensions, aggDimensions));
    }
}
