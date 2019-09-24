// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.table.resolver.QueryPlanningConstraint;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Methods to support table operations and table resolution.
 */
public final class TableUtils {

    /**
     * Get the fact store column names from the dimensions and metrics.
     * <p>
     * NOTE: This method exists since TemplateDruidQuery's don't have a valid DataSource from which we can
     *       retrieve a valid PhysicalTable. Consequently, at time of resolution, the table should be passed in.
     *
     * @param request  A request which supplies grouping dimensions and filtering dimensions
     * @param query  A query model which has metric column and possibly dimension column names
     * @param table  Physical table used in resolving dimension names
     *
     * @return a set of strings representing fact store column names
     *
     * @deprecated in favor of getColumnNames(DataApiRequest, DruidAggregationQuery) returning dimension api name
     */
    @Deprecated
    public static Set<String> getColumnNames(
            DataApiRequest request,
            DruidAggregationQuery<?> query,
            PhysicalTable table
    ) {
        return Stream.of(
                getDimensions(request, query)
                        .map(Dimension::getApiName)
                        .map(table::getPhysicalColumnName),
                query.getDependentFieldNames().stream()
        ).flatMap(Function.identity()).collect(Collectors.toSet());
    }

    /**
     * Get the schema column names from the dimensions and metrics.
     *
     * @param request  A request which supplies grouping dimensions and filtering dimensions
     * @param query  A query model which has metric column and possibly dimension column names
     *
     * @return a set of strings representing schema column names
     */
    public static Set<String> getColumnNames(DataApiRequest request, DruidAggregationQuery<?> query) {
        return Stream.concat(
                getDimensions(request, query).map(Dimension::getApiName),
                query.getDependentFieldNames().stream()
        ).collect(Collectors.toSet());
    }

    /**
     * Get a stream returning all the fact store dimensions.
     *
     * @param request A request which supplies grouping dimensions and filtering dimensions
     * @param query A query model which may have dimension column names
     *
     * @return a set of strings representing fact store column names
     */
    public static Stream<Dimension> getDimensions(DataApiRequest request, DruidAggregationQuery<?> query) {
        return Stream.of(
                request.getDimensions().stream(),
                request.getApiFilters().keySet().stream(),
                query.getMetricDimensions().stream()
        ).flatMap(Function.identity());
    }

    /**
     * Returns union of constrained availabilities of constrained logical table.
     *
     * @param logicalTable  The constrained logical table
     * @param queryPlanningConstraint  The constraint
     *
     * @return the union of constrained availabilities of constrained logical table
     */
    public static SimplifiedIntervalList getConstrainedLogicalTableAvailability(
            LogicalTable logicalTable,
            QueryPlanningConstraint queryPlanningConstraint
    ) {
        return logicalTable.getTableGroup().getPhysicalTables().stream()
                .map(physicalTable -> physicalTable.withConstraint(queryPlanningConstraint))
                .map(PhysicalTable::getAvailableIntervals)
                .reduce(new SimplifiedIntervalList(), SimplifiedIntervalList::union);
    }

    /**
     * Returns union of availabilities of the logical table.
     *
     * @param logicalTable  The logical table
     *
     * @return the union of availabilities of the logical table
     */
    public static SimplifiedIntervalList logicalTableAvailability(LogicalTable logicalTable) {
        return logicalTable.getTableGroup().getPhysicalTables().stream()
                .map(PhysicalTable::getAllAvailableIntervals)
                .map(Map::entrySet)
                .flatMap(Set::stream)
                .map(Map.Entry::getValue)
                .reduce(new SimplifiedIntervalList(), SimplifiedIntervalList::union);
    }
}
