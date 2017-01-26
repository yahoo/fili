// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider.impl;

import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.druid.model.query.Granularity;

import java.util.function.Predicate;

/**
 * An ApiMetricName built from configuration.
 *
 * Perhaps not totally complete:
 * - Should there be a distinction between apiName and asName?
 */
public class ApiMetricNameImpl implements ApiMetricName {
    protected final String name;
    protected final Predicate<Granularity> validFor;

    /**
     * Construct a new configured API metric name.
     *
     * @param name  the name
     * @param validFor  returns true if valid for given granularity
     */
    public ApiMetricNameImpl(String name, Predicate<Granularity> validFor) {
        this.name = name;
        this.validFor = validFor;
    }

    @Override
    public boolean isValidFor(final TimeGrain grain) {
        return validFor.test(grain);
    }

    @Override
    public String getApiName() {
        return name;
    }

    @Override
    public String asName() {
        return name;
    }

    @Override
    public String toString() {
        return "ApiMetricNameImpl [" + name + "]";
    }
}
