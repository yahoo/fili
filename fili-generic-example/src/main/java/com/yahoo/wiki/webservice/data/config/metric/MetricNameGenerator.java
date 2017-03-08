package com.yahoo.wiki.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.time.TimeGrain;

import java.util.Arrays;
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

    public static FiliMetricName getFiliMetricName(String name) {
        if (defaultTimeGrain == null) {
            throw new RuntimeException("Default time grain must be specified if no time grains are given");
        }
        return getFiliMetricName(name, defaultTimeGrain);
    }

    public static FiliMetricName getFiliMetricName(String name, TimeGrain... timeGrains) {
        List<TimeGrain> timeGrainList = Arrays.asList(timeGrains);
        if (defaultTimeGrain != null && !timeGrainList.contains(defaultTimeGrain)) {
            timeGrainList.add(defaultTimeGrain);
        }
        return new FiliMetricName(name, timeGrainList);
    }
}
