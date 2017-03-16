// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.time.TimeGrain;

import java.util.List;

/**
 * Created by kevin on 3/7/2017.
 */
public class FiliApiMetricName implements ApiMetricName {
    private final String apiName;
    private final List<TimeGrain> satisfyingGrains;

    public FiliApiMetricName(String name, List<TimeGrain> timeGrains) {
        this.apiName = name;
        satisfyingGrains = timeGrains;
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
        return satisfyingGrains.stream().anyMatch(grain::satisfiedBy);
    }

    @Override
    public String getApiName() {
        return toString();
    }
}
