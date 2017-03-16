// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.config.names.FieldName;

/**
 * Created by kevin on 3/7/2017.
 */
public class DruidMetricName implements FieldName {
    private final String name;

    public DruidMetricName(String name) {
        this.name = name;
    }

    @Override
    public String asName() {
        return toString();
    }

    @Override
    public String toString() {
        return name;
    }
}
