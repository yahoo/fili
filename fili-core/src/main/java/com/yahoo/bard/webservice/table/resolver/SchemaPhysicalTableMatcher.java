// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TABLE_SCHEMA_UNDEFINED;

import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *  Use the granularity and columns of a query to determine whether or not tables can satisfy this query.
 */
public class SchemaPhysicalTableMatcher implements PhysicalTableMatcher {

    public static final Logger LOG = LoggerFactory.getLogger(SchemaPhysicalTableMatcher.class);

    public static final ErrorMessageFormat MESSAGE_FORMAT = TABLE_SCHEMA_UNDEFINED;

    private final QueryPlanningConstraint requestConstraint;

    /**
     * Constructor saves metrics, dimensions, coarsest time grain, and logical table name (for logging).
     *
     * @param requestConstraint contains the request constraints extracted from DataApiRequest and TemplateDruidQuery
     */
    public SchemaPhysicalTableMatcher(QueryPlanningConstraint requestConstraint) {
        this.requestConstraint = requestConstraint;
    }

    @Override
    public boolean test(PhysicalTable table) {
        if (!requestConstraint.getMinimumGranularity().satisfiedBy(table.getSchema().getGranularity())) {
            return false;
        }

        Set<String> supplyNames = table.getSchema().getColumns().stream()
                .map(Column::getName)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Set<String> columnNames = requestConstraint.getAllColumnNames();

        return supplyNames.containsAll(columnNames);
    }

    @Override
    public NoMatchFoundException noneFoundException() {
        String logicalTableName = requestConstraint.getLogicalTable().getName();
        Set<String> logicalMetrics = requestConstraint.getLogicalMetricNames();
        Set<String> dimensions = requestConstraint.getAllDimensionNames();
        String grainName = requestConstraint.getMinimumGranularity().getName();

        LOG.error(MESSAGE_FORMAT.logFormat(logicalTableName, dimensions, logicalMetrics, grainName));

        return new NoMatchFoundException(
                MESSAGE_FORMAT.format(logicalTableName, dimensions, logicalMetrics, grainName));
    }
}
