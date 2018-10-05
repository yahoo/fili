// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.builders;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.web.ApiHaving;

import java.util.Map;
import java.util.Set;

/**
 *  A Single Abstract Method interface to support building DruidHavings.
 */
@FunctionalInterface
public interface DruidHavingBuilder {
    /**
     * Build a having model that ANDs together having queries for each of the metrics.
     *
     * @param apiHavingMap  A data api request with a bound ApiHavings clause
     *
     * @return The having clause to appear in the Druid query. Returns null if the metricMap is empty or null.
     */
    Having buildHavings(Map<LogicalMetric, Set<ApiHaving>> apiHavingMap);
}
