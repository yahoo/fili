// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.util;

import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;

/**
 * A subclass of QueryWeightUtil that never skips weigh checking.
 */
public class QueryWeightUtiAlwaysRun extends QueryWeightUtil {

    public QueryWeightUtiAlwaysRun() {
        super();
    }

    @Override
    public boolean skipWeightCheckQuery(DruidAggregationQuery<?> query) {
        return false;
    }
}
