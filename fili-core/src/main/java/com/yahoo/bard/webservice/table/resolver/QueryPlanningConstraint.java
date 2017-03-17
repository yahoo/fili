// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.web.DataApiRequest;

import org.joda.time.Interval;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

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
     * @param dataApiRequest Api request containing the constraints information.
     * @param templateDruidQuery Query containing metric constraint information.
     */
    public QueryPlanningConstraint(DataApiRequest dataApiRequest, TemplateDruidQuery templateDruidQuery) {
        super(dataApiRequest, templateDruidQuery);

        this.logicalTable = dataApiRequest.getTable();
        this.intervals = Collections.unmodifiableSet(dataApiRequest.getIntervals());
        this.logicalMetrics = Collections.unmodifiableSet(dataApiRequest.getLogicalMetrics());
        this.minimumGranularity = new RequestQueryGranularityResolver().apply(dataApiRequest, templateDruidQuery);
        this.requestGranularity = dataApiRequest.getGranularity();
        this.logicalMetricNames = Collections.unmodifiableSet(this.logicalMetrics.stream()
                .map(LogicalMetric::getName).collect(Collectors.toSet()));
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
}
