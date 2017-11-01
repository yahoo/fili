// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.config.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.aggregation.CardinalityAggregation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Metric Maker to calculate the cardinality.
 */
public class CardinalityMaker extends MetricMaker {

    private static final Logger LOG = LoggerFactory.getLogger(MetricMaker.class);

    private static final int DEPENDENT_METRICS_REQUIRED = 1;

    private final DimensionDictionary dimensionDictionary;
    private final boolean byRow;

    /**
     * Constructor.
     *
     * @param metrics  Unused reference to metrics dictionary
     * @param dimensionDictionary  Dictionary to resolve dimension api names against
     * @param byRow  Cardinality by value or row
     */
    public CardinalityMaker(
            MetricDictionary metrics,
            DimensionDictionary dimensionDictionary,
            boolean byRow
    ) {
        super(metrics);
        this.dimensionDictionary = dimensionDictionary;
        this.byRow = byRow;
    }

    /**
     * Cardinality metrics depend on dimensions not metrics.
     *
     * @param dependentDimensions  The dimension names for this cardinality metric
     */
    @Override
    protected void assertDependentMetricsExist(List<String> dependentDimensions) {
        for (String dependentDimension : dependentDimensions) {
            if (dimensionDictionary.findByApiName(dependentDimension) == null) {
                String message = String.format(
                        "Cardinality metric dependent dimension %s is not in the dimension dictionary",
                        dependentDimension);
                LOG.error(message);
                throw new IllegalArgumentException(message);
            }
        }
    }

    @Override
    protected LogicalMetric makeInner(LogicalMetricInfo logicalMetricInfo, List<String> dependentDimensions) {
        Set<Dimension> dimensions = dependentDimensions.stream()
                .map(dimensionDictionary::findByApiName)
                .collect(Collectors.toSet());

        Set<Aggregation> aggs = Collections.singleton(
                new CardinalityAggregation(logicalMetricInfo.getName(), dimensions, byRow)
        );

        return new LogicalMetric(new TemplateDruidQuery(aggs, Collections.emptySet()), NO_OP_MAPPER, logicalMetricInfo);
    }

    @Override
    protected int getDependentMetricsRequired() {
        return DEPENDENT_METRICS_REQUIRED;
    }
}
