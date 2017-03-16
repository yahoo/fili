// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.time.TimeGrain;

import java.util.List;

/**
 *
 */
public class MetricNameGenerator {

    public static DruidMetricName getDruidMetric(String name) {
        return new DruidMetricName(name);
    }

    public static FiliApiMetricName getFiliMetricName(String name, List<TimeGrain> timeGrains) {
        return new FiliApiMetricName(name, timeGrains);
    }
}
