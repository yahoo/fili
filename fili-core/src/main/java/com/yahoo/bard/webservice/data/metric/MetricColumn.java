// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric;

import com.yahoo.bard.webservice.table.Column;

/**
 * Metric.
 */
public class MetricColumn extends Column {

    /**
     * Constructor.
     *
     * @param name  The column name
     */
    public MetricColumn(String name) {
        super(name);
    }

    @Override
    public String toString() {
        return "{metric:'" + getName() + "'}";
    }
}
