// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.luthier.names;

import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.TimeGrain;

import java.util.List;
import java.util.function.Predicate;


/**
 * For Luthier Configuration.
 * Supplies an always_true predicate for Granularity when we want the metricName to be valid for any granularity
 */
public class LuthierApiMetricName implements ApiMetricName {
    private final String apiName;
    private final Predicate<Granularity> granularityPredicate;
    private static final Predicate<Granularity> ALWAYS_TRUE = granularity -> true;

    /**
     * Constructs a FiliApiMetricName.
     *
     * @param name  The name of the metric.
     * @param granularities  a list of acceptable granularities
     */
    public LuthierApiMetricName(String name, List<Granularity> granularities) {
        this.apiName = name;
        this.granularityPredicate = granularities::contains;
    }

    /**
     * Constructs a FiliApiMetricName.
     *
     * @param name  The name of the metric.
     */
    public LuthierApiMetricName(String name) {
        this.apiName = name;
        this.granularityPredicate = ALWAYS_TRUE;
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
        return granularityPredicate.test(grain);
    }

    @Override
    public boolean isValidFor(Granularity granularity) {
        return granularityPredicate.test(granularity);
    }

    @Override
    public String getApiName() {
        return toString();
    }
}
