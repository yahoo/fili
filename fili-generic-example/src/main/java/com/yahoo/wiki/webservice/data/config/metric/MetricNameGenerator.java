package com.yahoo.wiki.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.time.TimeGrain;

import java.util.Collections;
import java.util.List;

/**
 * Created by kevin on 2/28/2017.
 */
public class MetricNameGenerator {
    private static TimeGrain defaultTimeGrain;

    public static void setDefaultTimeGrain(TimeGrain t) {
        defaultTimeGrain = t;
    }

    public static DruidMetricName getDruidMetric(String name) {
        return new DruidMetricName(name);
    }

    public static FiliApiMetricName getFiliMetricName(String name) {
        if (defaultTimeGrain == null) {
            throw new RuntimeException("Default time grain must be specified if no time grains are given");
        }
        return getFiliMetricName(name, Collections.singletonList(defaultTimeGrain));
    }

    public static FiliApiMetricName getFiliMetricName(String name, List<TimeGrain> timeGrains) {
        if (defaultTimeGrain != null && !timeGrains.contains(defaultTimeGrain)) {
            timeGrains.add(defaultTimeGrain);
        }
        return new FiliApiMetricName(name, timeGrains);
    }
}
