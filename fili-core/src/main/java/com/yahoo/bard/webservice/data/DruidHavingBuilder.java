// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.druid.model.builders.DefaultDruidHavingBuilder;
import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.web.ApiHaving;

import java.util.Map;
import java.util.Set;

/**
 * Legacy DruidHavingBuilder in place for migration purposes.
 *
 * @deprecated Use {@link DefaultDruidHavingBuilder}
 */
@Deprecated
public class DruidHavingBuilder {

    /**
     * Build a having model that ANDs together having queries for each of the metrics.
     *
     * @param apiHavingMap  A map of logical metric to the set of havings for that metric
     *
     * @return The having clause to appear in the Druid query. Returns null if the metricMap is empty or null.
     */
    public static Having buildHavings(final Map<LogicalMetric, Set<ApiHaving>> apiHavingMap) {
        return DefaultDruidHavingBuilder.INSTANCE.buildHavings(apiHavingMap);
    }
}
