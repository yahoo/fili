// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.names;

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR;

import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.util.EnumUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Hold the list of API metric names.
 */
public enum WikiApiMetricName implements ApiMetricName {
    COUNT,
    ADDED,
    DELTA,
    DELETED;

    private final String apiName;
    private final List<TimeGrain> satisfyingGrains;

    /**
     * Create a Wiki Metric descriptor.
     */
    WikiApiMetricName() {
        this((String) null, HOUR);
    }

    /**
     * Create a Wiki Metric descriptor with a fixed set of satisfying grains.
     *
     * @param apiName  The api name for the metric.
     * @param satisfyingGrains  The grains that satisfy this metric.
     */
    WikiApiMetricName(String apiName, TimeGrain... satisfyingGrains) {
        // to camelCase
        this.apiName = (apiName == null ? EnumUtils.camelCase(this.name()) : apiName);
        this.satisfyingGrains = Arrays.asList(satisfyingGrains);
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ENGLISH);
    }

    @Override
    public String getApiName() {
        return apiName;
    }

    @Override
    public String asName() {
        return getApiName();
    }

    @Override
    public boolean isValidFor(TimeGrain grain) {
        // As long as the satisfying grains of this metric satisfy the requested grain
        return satisfyingGrains.stream().anyMatch(grain::satisfiedBy);
    }
}
