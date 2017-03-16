package com.yahoo.wiki.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.time.TimeGrain;

import java.util.List;

/**
 * Created by kevin on 2/28/2017.
 */
public class MetricNameGenerator {

    public static DruidMetricName getDruidMetric(String name) {
        return new DruidMetricName(name);
    }

    public static FiliApiMetricName getFiliMetricName(String name, List<TimeGrain> timeGrains) {
        return new FiliApiMetricName(name, timeGrains);
    }
}
