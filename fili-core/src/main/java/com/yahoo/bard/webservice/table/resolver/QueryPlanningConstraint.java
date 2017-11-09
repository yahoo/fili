// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.TablesApiRequest;
import com.yahoo.bard.webservice.web.filters.ApiFilters;

import org.joda.time.Interval;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * Constraints used to match and resolve the best table for a given query.
 */
public class QueryPlanningConstraint extends DataSourceConstraint {

    private final LogicalTable logicalTable;
    private final Set<Interval> intervals;
    private final Set<LogicalMetric> logicalMetrics;
    private final Granularity minimumGranularity;
    private final Granularity requestGranularity;
    private final Set<String> logicalMetricNames;

    /**
     * Constructor.
     *
     * @param requestDimensions  Dimensions contained in request
     * @param filterDimensions  Filtered dimensions
     * @param metricDimensions  Metric related dimensions
     * @param metricNames  Names of metrics
     * @param apiFilters  Map of dimension to its set of API filters
     * @param logicalTable  The logical table requested by the request
     * @param intervals  The interval constraint of the request
     * @param logicalMetrics  The logical metrics requested by the request
     * @param minimumGranularity  The finest granularity that must be satisfied by table granularity
     * @param requestGranularity  The requested granularity of on the requested table
     */
    public QueryPlanningConstraint(
            Set<Dimension> requestDimensions,
            Set<Dimension> filterDimensions,
            Set<Dimension> metricDimensions,
            Set<String> metricNames,
            ApiFilters apiFilters,
            LogicalTable logicalTable,
            Set<Interval> intervals,
            Set<LogicalMetric> logicalMetrics,
            Granularity minimumGranularity,
            Granularity requestGranularity
    ) {
        super(requestDimensions, filterDimensions, metricDimensions, metricNames, apiFilters);
        this.logicalTable = logicalTable;
        this.intervals = intervals;
        this.logicalMetrics = logicalMetrics;
        this.minimumGranularity = minimumGranularity;
        this.requestGranularity = requestGranularity;
        this.logicalMetricNames = generateLogicalMetricNames();
    }

    /**
     * Constructor.
     *
     * @param dataApiRequest  <b>Data API request</b> containing the constraints information
     * @param templateDruidQuery  Query containing metric constraint information
     */
    public QueryPlanningConstraint(
            @NotNull DataApiRequest dataApiRequest,
            @NotNull TemplateDruidQuery templateDruidQuery
    ) {
        super(dataApiRequest, templateDruidQuery);

        this.logicalTable = dataApiRequest.getTable();
        this.intervals = Collections.unmodifiableSet(dataApiRequest.getIntervals());
        this.logicalMetrics = Collections.unmodifiableSet(dataApiRequest.getLogicalMetrics());
        this.minimumGranularity = new RequestQueryGranularityResolver().apply(dataApiRequest, templateDruidQuery);
        this.requestGranularity = dataApiRequest.getGranularity();
        this.logicalMetricNames = generateLogicalMetricNames();
    }

    /**
     * Constructor.
     *
     * @param tablesApiRequest  <b>Tables API request</b> containing the constraints information.
     */
    public QueryPlanningConstraint(@NotNull TablesApiRequest tablesApiRequest) {
        super(
                tablesApiRequest.getDimensions(),
                tablesApiRequest.getFilterDimensions(),
                Collections.emptySet(),
                Collections.emptySet(),
                tablesApiRequest.getApiFilters()
        );
        this.logicalTable = tablesApiRequest.getTable();
        this.intervals = Collections.unmodifiableSet(tablesApiRequest.getIntervals());
        this.logicalMetrics = Collections.unmodifiableSet(tablesApiRequest.getLogicalMetrics());
        this.minimumGranularity = tablesApiRequest.getGranularity();
        this.requestGranularity = tablesApiRequest.getGranularity();
        this.logicalMetricNames = generateLogicalMetricNames();
    }

    public LogicalTable getLogicalTable() {
        return logicalTable;
    }

    public Set<Interval> getIntervals() {
        return intervals;
    }

    public Set<LogicalMetric> getLogicalMetrics() {
        return logicalMetrics;
    }

    public Set<String> getLogicalMetricNames() {
        return logicalMetricNames;
    }

    public Granularity getMinimumGranularity() {
        return minimumGranularity;
    }

    public Granularity getRequestGranularity() {
        return requestGranularity;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof QueryPlanningConstraint) {
            QueryPlanningConstraint that = (QueryPlanningConstraint) obj;
            return super.equals(that)
                    && Objects.equals(this.logicalTable, that.logicalTable)
                    && Objects.equals(this.intervals, that.intervals)
                    && Objects.equals(this.logicalMetrics, that.logicalMetrics)
                    && Objects.equals(this.minimumGranularity, that.minimumGranularity)
                    && Objects.equals(this.requestGranularity, that.requestGranularity)
                    && Objects.equals(this.logicalMetricNames, that.logicalMetricNames);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                logicalTable,
                intervals,
                logicalMetrics,
                minimumGranularity,
                requestGranularity,
                logicalMetricNames
        );
    }

    /**
     * Return names of all {@link #logicalMetrics}.
     *
     * @return names of all {@link #logicalMetrics}
     */
    private Set<String> generateLogicalMetricNames() {
        return Collections.unmodifiableSet(
                logicalMetrics.stream()
                        .map(LogicalMetric::getName)
                        .collect(Collectors.toSet())
        );
    }
}
