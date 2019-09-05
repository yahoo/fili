// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.time.TimeGrain;

/**
 * Holds a metric name which is stored in fili.
 */
public class FiliApiMetricName implements ApiMetricName {
    private final String apiName;
    private final TimeGrain timeGrain;

    /**
     * Constructs a FiliApiMetricName.
     *
     * @param name  The name of the metric.
     * @param timeGrain  A valid timegrain for this metric.
     */
    public FiliApiMetricName(String name, TimeGrain timeGrain) {
        this.apiName = name;
        this.timeGrain = timeGrain;
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
        return grain.satisfiedBy(timeGrain);
    }

    @Override
    public String getApiName() {
        return toString();
    }
}
