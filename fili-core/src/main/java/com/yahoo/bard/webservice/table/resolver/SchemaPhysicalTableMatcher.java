// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TABLE_SCHEMA_UNDEFINED;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.util.TableUtils;
import com.yahoo.bard.webservice.web.DataApiRequest;
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

    private final DataApiRequest request;
    private final Granularity granularity;
    private final TemplateDruidQuery query;

    /**
     * Constructor saves metrics, dimensions, coarsest time grain, and logical table name (for logging).
     *
     * @param request  The request whose dimensions are being matched on
     * @param query  The query whose columns are being matched
     * @param granularity  The granularity that tables under test must satisfy
     */
    public SchemaPhysicalTableMatcher(DataApiRequest request, TemplateDruidQuery query, Granularity granularity) {
        this.request = request;
        this.granularity = granularity;
        this.query = query;
    }

    @Override
    public boolean test(PhysicalTable table) {
        if (!granularity.satisfiedBy(table.getSchema().getGranularity())) {
            return false;
        }

        Set<String> supplyNames = table.getSchema().getColumns().stream()
                .map(Column::getName)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Set<String> columnNames = TableUtils.getColumnNames(request, query.getInnermostQuery());

        return supplyNames.containsAll(columnNames);
    }

    @Override
    public NoMatchFoundException noneFoundException() {
        String logicalTableName = request.getTable().getName();
        Set<String> logicalMetrics = request.getLogicalMetrics().stream()
                .map(LogicalMetric::getName)
                .collect(Collectors.toSet());
        Set<String> dimensions = request.getDimensions().stream()
                .map(Dimension::getApiName)
                .collect(Collectors.toSet());

        LOG.error(MESSAGE_FORMAT.logFormat(logicalTableName, dimensions, logicalMetrics, granularity.getName()));
        return new NoMatchFoundException(
                MESSAGE_FORMAT.format(logicalTableName, dimensions, logicalMetrics, granularity.getName())
        );
    }
}
