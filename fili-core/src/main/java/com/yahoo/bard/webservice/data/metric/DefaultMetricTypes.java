// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric;

/**
 * Static instances of standard metric types.
 */
public class DefaultMetricTypes {
    public static final String TYPE_DEFAULT_NAME = "number";
    public static final String PERCENT_SUBTYPE = "percentage";
    public static final String SKETCH_NAME = "sketch";
    public static final String SKETCH_DEFAULT_SUBTYPE = "theta";

    public static final MetricType NUMBER = new MetricType(TYPE_DEFAULT_NAME);
    public static final MetricType PERCENTAGE = new MetricType(TYPE_DEFAULT_NAME, PERCENT_SUBTYPE);
    public static final MetricType SKETCH = new MetricType(SKETCH_NAME, SKETCH_DEFAULT_SUBTYPE);
}
