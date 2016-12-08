// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.aggregation.FilteredAggregation;
import com.yahoo.bard.webservice.druid.model.filter.Filter;

import java.util.Collections;
import java.util.List;

/**
 * Build a Filtered Aggregation logical metric.
 */
public class FilteredAggregationMaker extends MetricMaker {

    protected final Filter filter;
    protected final MetricDictionary metricDictionary;
    protected final Aggregation aggregation;

    /**
     * Construct a metric maker for Filtered Aggregations.
     * <p>
     * @param metricDictionary the metric dictionary
     * @param aggregation the underlying aggregation
     * @param filter the filter to filter the aggregation by
     */
    public FilteredAggregationMaker(MetricDictionary metricDictionary, Aggregation aggregation, Filter filter) {
        super(metricDictionary);
        this.aggregation = aggregation;
        this.filter = filter;
        this.metricDictionary = metricDictionary;
    }

    @Override
    protected LogicalMetric makeInner(String metricName, List<String> dependentMetrics) {
        FilteredAggregation filteredAggregation = new FilteredAggregation(
                metricName,
                aggregation.getFieldName(),
                aggregation,
                filter
        );
        return new LogicalMetric(
                new TemplateDruidQuery(Collections.singleton(filteredAggregation), Collections.emptySet()),
                new NoOpResultSetMapper(),
                metricName
        );
    }

    @Override
    protected int getDependentMetricsRequired() {
        return 0;
    }
}
