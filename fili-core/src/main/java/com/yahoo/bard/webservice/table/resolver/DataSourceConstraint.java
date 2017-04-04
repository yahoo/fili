// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.data.dimension.Dimension;
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
     * Copy Constructor.
     *
     * @param dataSourceConstraint  The data source constarint to copy from
     */
    public DataSourceConstraint(DataSourceConstraint dataSourceConstraint) {
        this.requestDimensions = dataSourceConstraint.getRequestDimensions();
        this.filterDimensions = dataSourceConstraint.getFilterDimensions();
        this.metricDimensions = dataSourceConstraint.getMetricDimensions();
        this.metricNames = dataSourceConstraint.getMetricNames();
        this.apiFilters = dataSourceConstraint.getApiFilters();
        this.allDimensions = dataSourceConstraint.getAllDimensions();
        this.allDimensionNames = this.getAllDimensionNames();
        this.allColumnNames = this.getAllColumnNames();
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
}
