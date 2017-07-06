// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.time.TimeGrain;

import java.util.Collections;
import java.util.List;

/**
 * Holds a metric name which is stored in fili.
 */
public class FiliApiMetricName implements ApiMetricName {
    private final String apiName;
    private final List<TimeGrain> timeGrains;

    /**
     * Constructs a FiliApiMetricName.
     *
     * @param apiName  The apiName of the metric.
     * @param timeGrain  A valid timegrain for this metric.
     */
    public FiliApiMetricName(String apiName, TimeGrain timeGrain) {
        this(apiName, Collections.singletonList(timeGrain));
    }

    public FiliApiMetricName(String apiName, List<TimeGrain> timeGrains) {
        this.apiName = apiName;
        this.timeGrains = timeGrains;
    }

    @Override
    public String asName() {
        return getApiName();
    }

    @Override
    public String toString() {
        return apiName;
    }

    @Override
    public boolean isValidFor(TimeGrain grain) {
        return timeGrains.stream().anyMatch(grain::satisfiedBy);
    }

    @Override
    public String getApiName() {
        return toString();
    }
}
