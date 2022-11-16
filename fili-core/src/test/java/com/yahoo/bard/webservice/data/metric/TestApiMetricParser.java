// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric;

import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;
import com.yahoo.bard.webservice.web.apirequest.generator.metric.ApiMetricParser;
import com.yahoo.bard.webservice.web.apirequest.metrics.ApiMetric;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Test implementation of metric parser.
 * // TODO elaborate
 */
public class TestApiMetricParser implements ApiMetricParser {

    /**
     * {@code ApiMetricParser#apply} returns a copy of this field.
     */
    private List<ApiMetric> resultMetrics = new ArrayList<>();

    /**
     * Predicate to validate {@code metricQuery} parameter in {@code apply} method.
     */
    private Predicate<String> metricQueryCheck = unused -> true;

    @Override
    public List<ApiMetric> apply(String metricQuery) throws BadApiRequestException {
        if (!metricQueryCheck.test(metricQuery)) {
            throw new BadApiRequestException("Bad metric query: " + metricQuery);
        }
        return new ArrayList<>(resultMetrics);
    }

    /**
     * Sets the result of the {@link TestApiMetricParser#apply} method.
     *
     * @param resultMetrics  The list of {@link ApiMetric}s to be returned from the {@code apply} method.
     */
    public void setResultMetrics(List<ApiMetric> resultMetrics) {
        this.resultMetrics = new ArrayList<>(resultMetrics);
    }

    /**
     * Sets a predicate to validate the input of the {@link TestApiMetricParser#apply} method. By default no check is
     * done.
     *
     * @param check  A check to run against the input metric query. Must return "true" if the metric query is valid.
     *               Returning false will cause a {@link BadApiRequestException} to be thrown.
     */
    public void setMetricQueryCheck(Predicate<String> check) {
        this.metricQueryCheck = check;
    }
}
