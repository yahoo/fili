// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.builders;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.HAVING_OPERATOR_IMPROPER_RANGE;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.HAVING_OPERATOR_WRONG_NUMBER_OF_PARAMETERS;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.druid.model.having.AndHaving;
import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.druid.model.having.NotHaving;
import com.yahoo.bard.webservice.druid.model.having.NumericHaving;
import com.yahoo.bard.webservice.druid.model.having.OrHaving;
import com.yahoo.bard.webservice.web.ApiHaving;
import com.yahoo.bard.webservice.web.HavingOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class to hold static methods to build druid query model objects from ApiHaving.
 */
public class DefaultDruidHavingBuilder implements DruidHavingBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDruidHavingBuilder.class);
    private static final int HAVING_RANGE_PARAM_LENGTH = 2;

    public static final DefaultDruidHavingBuilder INSTANCE = new DefaultDruidHavingBuilder();

    /**
     * Build a having model that ANDs together having queries for each of the metrics.
     *
     * @param apiHavingMap  A map of logical metric to the set of havings for that metric
     *
     * @return The having clause to appear in the Druid query. Returns null if the metricMap is empty or null.
     */
    @Override
    public Having buildHavings(Map<LogicalMetric, Set<ApiHaving>> apiHavingMap) {
        // return null when no metrics are specified in the API
        if (apiHavingMap == null || apiHavingMap.isEmpty()) {
            return null;
        }

        List<Having> havings = apiHavingMap.entrySet().stream()
                .map(entry -> buildMetricHaving(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
        // for a single having just return the entry
        Having newHaving = havings.size() == 1 ? havings.get(0) : new AndHaving(havings);
        LOG.trace("Having: {}", newHaving);
        return newHaving;
    }

    /**
     * Build a Having for all the having queries for a single metric, ANDing them together.
     *
     * @param metric  Metric for the having query
     * @param havings  All having queries belonging to that metric
     *
     * @return A druid query having object representing the having clause on a given metric
     */
    public Having buildMetricHaving(LogicalMetric metric, Set<ApiHaving> havings) {
        LOG.trace("Building metric having using metric: {} \n\n and set of queries: {}", metric, havings);
        List<Having> orHavings = havings.stream()
                .map(having -> buildHaving(metric, having))
                .collect(Collectors.toList());
        Having newHaving = orHavings.size() == 1 ? orHavings.get(0) : new AndHaving(orHavings);
        LOG.trace("Filter: {}", newHaving);
        return newHaving;
    }

    /**
     * Create a list of NumericHavings for the values specified and OR them together.
     *
     * @param metric  The metric that the operation applied to.
     * @param having  The ApiHaving object
     *
     * @return A single having representing the API Filter
     */
    public Having buildHaving(LogicalMetric metric, ApiHaving having) {
        LOG.trace("Building having using metric: {} and API Having: {}", metric, having);

        HavingOperation operation = having.getOperation();
        List<Double> values = having.getValues();

        Having newHaving;
        if (operation.equals(HavingOperation.between) || operation.equals(HavingOperation.notBetween)) {
            if (values.size() != HAVING_RANGE_PARAM_LENGTH) {
                throw new UnsupportedOperationException(HAVING_OPERATOR_WRONG_NUMBER_OF_PARAMETERS.format
                        (operation.name(), operation.name(), HAVING_RANGE_PARAM_LENGTH, values.size()));
            }
            double lowerValue = values.get(0);
            double upperValue = values.get(1);
            if (upperValue < lowerValue) {
                throw new IllegalArgumentException(HAVING_OPERATOR_IMPROPER_RANGE.format(operation.name()));
            }
            List<Having> orHavings = new ArrayList<>();
            orHavings.add(new NumericHaving(Having.DefaultHavingType.GREATER_THAN, metric.getName(),
                    lowerValue));
            orHavings.add(new NumericHaving(Having.DefaultHavingType.EQUAL_TO, metric.getName(), lowerValue));
            OrHaving orHavingLower = new OrHaving(orHavings);
            orHavings = new ArrayList<>();
            orHavings.add(new NumericHaving(Having.DefaultHavingType.LESS_THAN, metric.getName(), upperValue));
            orHavings.add(new NumericHaving(Having.DefaultHavingType.EQUAL_TO, metric.getName(), upperValue));
            OrHaving orHavingUpper = new OrHaving(orHavings);
            newHaving = new AndHaving(Arrays.asList(orHavingLower, orHavingUpper));
        }
        else {
            List<Having> havings = having.getValues().stream()
                    .map(value -> new NumericHaving(operation.getType(), metric.getName(), value))
                    .collect(Collectors.toList());
            newHaving = havings.size() == 1 ? havings.get(0) : new OrHaving(havings);
        }

        // Negate the outer having to preserve expected semantics of negated queries.
        // For example, the having
        //     metric1-notEqualTo[2,4,8]
        // can either be parsed as
        //     !(metric1 == 2) || !(metric1 == 4) || !(metric1 == 8)
        // or
        //     !(metric1 == 2 || metric1 == 4 || metric1 == 8)
        // . The having
        //     metric1-notGreaterThan[2,4,8]
        // can either be parsed as
        //     !(metric1 > 2) || !(metric1 > 4) || !(metric1 > 8)
        // or
        //     !(metric1 > 2 || metric1 > 4 || metric1 > 8)
        // . In both cases, the second method is correct.

        return operation.isNegated() ? new NotHaving(newHaving) : newHaving;
    }
}
