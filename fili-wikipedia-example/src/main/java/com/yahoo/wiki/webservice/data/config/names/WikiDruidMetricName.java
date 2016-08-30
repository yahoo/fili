// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.names;

import com.yahoo.bard.webservice.data.config.names.FieldName;

import java.util.Locale;

/**
 * Hold the list of raw Druid metric names.
 */
public enum WikiDruidMetricName implements FieldName {
    COUNT,
    ADDED,
    DELTA,
    DELETED;

    private final String lowerCaseName;

    /**
     * Create a physical metric name instance.
     */
    WikiDruidMetricName() {
        this.lowerCaseName = name().toLowerCase(Locale.ENGLISH);
    }

    @Override
    public String toString() {
        return lowerCaseName;
    }

    @Override
    public String asName() {
        return toString();
    }
}
