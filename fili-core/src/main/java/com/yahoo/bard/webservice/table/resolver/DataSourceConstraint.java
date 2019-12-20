// Copyright 2019 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.web.filters.ApiFilters;

import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Constraint against a datasource for a query.
 */
public interface DataSourceConstraint {

    /**
     * Build a constraint which should not filter away any part of a given table.
     *
     * @param table  The table whose dimensions and metrics are to be queried
     *
     * @return a constraint which should provide no restrictions
     */
    static DataSourceConstraint unconstrained(PhysicalTable table) {
        return new BaseDataSourceConstraint(
                table.getDimensions(),
                Collections.emptySet(),
                Collections.emptySet(),
                table.getSchema().getMetricColumnNames(),
                table.getDimensions(),
                table.getDimensions().stream()
                        .map(Dimension::getApiName)
                        .collect(Collectors.toSet()),
                table.getSchema().getColumnNames(),
                new ApiFilters(Collections.emptyMap())
        );
    }

    /**
     * Getter the set of Dimensions required to satisfy the dimensions explictly requested in the query.
     *
     * @return the set of request dimensions
     */
    Set<Dimension> getRequestDimensions();

    /**
     * Getter for the set of Dimensions required to satisfy all filters.
     *
     * @return the set of filter dimensions
     */
    Set<Dimension> getFilterDimensions();

    /**
     * Getter for the set of Dimensions required to satisfy all metric dependencies.
     *
     * @return the set of metric dimensions
     */
    Set<Dimension> getMetricDimensions();

    /**
     * Getter for the set of all metric names required by this query.
     *
     * @return the set of metric names
     */
    Set<String> getMetricNames();

    /**
     * Getter for the set of all dimensions required by this query.
     *
     * @return the set of dimensions
     */
    Set<Dimension> getAllDimensions();

    /**
     * Getter for the api names of all dimensions required by this query.
     *
     * @return the dimension names.
     */
    Set<String> getAllDimensionNames();

    /**
     * Getter for the set of all column names required by this query. These column names must be satisfied by the
     * queried table's schema.
     *
     * @return the column names
     */
    Set<String> getAllColumnNames();

    /**
     * Getter for the set of Api filters that constrain this request.
     *
     * @return the api filters
     */
    ApiFilters getApiFilters();

    /**
     * Provides an view of this constraint intersected by the set of metric names.
     *
     * @param metricNames the metric names to intersect with this query
     * @return the resultant constraint
     */
    DataSourceConstraint withMetricIntersection(Set<String> metricNames);

    /**
     * Provides a view of this constraint with dimensions filtered by the provided predicate.
     *
     * @param filter the predicate which determined which dimensions should be exposed in the resultant view
     * @return the constraint filtered by the predicate
     */
    DataSourceConstraint withDimensionFilter(Predicate<Dimension> filter);
}
