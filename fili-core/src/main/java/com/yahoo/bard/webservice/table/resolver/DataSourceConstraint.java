// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.DataApiRequest;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Constraints for retrieving potential table availability for a given query.
 */
public class DataSourceConstraint {

    private final Set<Dimension> requestDimensions;
    private final Set<Dimension> filterDimensions;
    private final Set<Dimension> metricDimensions;
    private final Set<String> metricNames;
    private final Map<Dimension, Set<ApiFilter>> apiFilters;
    private final Set<Dimension> allDimensions;
    private final Set<String> allDimensionNames;
    private final Set<String> allColumnNames;

    /**
     * Constructor.
     *
     * @param dataApiRequest  Api request containing the constraints information.
     * @param templateDruidQuery  Query containing metric constraint information.
     */
    public DataSourceConstraint(DataApiRequest dataApiRequest, DruidAggregationQuery<?> templateDruidQuery) {
        this.requestDimensions = Collections.unmodifiableSet(dataApiRequest.getDimensions());
        this.filterDimensions = Collections.unmodifiableSet(dataApiRequest.getFilterDimensions());
        this.metricDimensions = Collections.unmodifiableSet(templateDruidQuery.getMetricDimensions());
        this.metricNames = Collections.unmodifiableSet(templateDruidQuery.getDependentFieldNames());
        this.apiFilters = Collections.unmodifiableMap(dataApiRequest.getFilters());
        this.allDimensions = Collections.unmodifiableSet(Stream.of(
                getRequestDimensions().stream(),
                getFilterDimensions().stream(),
                getMetricDimensions().stream()
        ).flatMap(Function.identity()).collect(Collectors.toSet()));
        this.allDimensionNames = Collections.unmodifiableSet(allDimensions.stream()
                .map(Dimension::getApiName)
                .collect(Collectors.toSet()));
        this.allColumnNames = Collections.unmodifiableSet(Stream.concat(
                allDimensionNames.stream(),
                metricNames.stream()
        ).collect(Collectors.toSet()));
    }

    /**
     * Constructor.
     *
     * @param requestDimensions  Dimensions contained in request
     * @param filterDimensions  Filtered dimensions
     * @param metricDimensions  Metric related dimensions
     * @param metricNames  Names of metrics
     * @param allDimensions  Set of all dimension objects
     * @param allDimensionNames  Set of all dimension names
     * @param allColumnNames  Set of all column names
     * @param apiFilters  Map of dimension to its set of API filters
     */
    public DataSourceConstraint(
            Set<Dimension> requestDimensions,
            Set<Dimension> filterDimensions,
            Set<Dimension> metricDimensions,
            Set<String> metricNames,
            Set<Dimension> allDimensions,
            Set<String> allDimensionNames,
            Set<String> allColumnNames,
            Map<Dimension, Set<ApiFilter>> apiFilters
    ) {
        this.requestDimensions = requestDimensions;
        this.filterDimensions = filterDimensions;
        this.metricDimensions = metricDimensions;
        this.metricNames = metricNames;
        this.allDimensions = allDimensions;
        this.allDimensionNames = allDimensionNames;
        this.allColumnNames = allColumnNames;
        this.apiFilters = apiFilters;
    }


    public Set<Dimension> getRequestDimensions() {
        return requestDimensions;
    }

    public Set<Dimension> getFilterDimensions() {
        return filterDimensions;
    }

    public Set<Dimension> getMetricDimensions() {
        return metricDimensions;
    }

    public Set<String> getMetricNames() {
        return metricNames;
    }

    public Set<Dimension> getAllDimensions() {
        return allDimensions;
    }

    public Set<String> getAllDimensionNames() {
        return allDimensionNames;
    }

    public Set<String> getAllColumnNames() {
        return allColumnNames;
    }

    public Map<Dimension, Set<ApiFilter>> getApiFilters() {
        return apiFilters;
    }

    /**
     * Create a new <tt>DataSourceConstraint</tt> instance with a new subset of metric names.
     * <p>
     * The new set of metric names will be an intersection between old metric names and
     * a user provided set of metric names
     *
     * @param metricColumns  The user provided set of metric names
     *
     * @return the new <tt>DataSourceConstraint</tt> instance with a new subset of metric names
     */
    public DataSourceConstraint withMetricIntersection(Set<MetricColumn> metricColumns) {
        return new DataSourceConstraint(
                requestDimensions,
                filterDimensions,
                metricDimensions,
                metricColumns.stream()
                        .filter(metricColumn -> metricNames.contains(metricColumn.getName()))
                        .map(MetricColumn::getName)
                        .collect(Collectors.toSet()),
                allDimensions,
                allDimensionNames,
                allColumnNames,
                apiFilters
        );
    }
}
