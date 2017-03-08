package com.yahoo.wiki.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.util.EnumUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Created by kevin on 2/28/2017.
 */
public class MetricNameGenerator {
    private static TimeGrain defaultTimeGrain;

    public static void setDefaultTimeGrain(TimeGrain t) {
        defaultTimeGrain = t;
    }

    public static DruidMetricName getDruidMetric(String name, TimeGrain... timeGrains) {
        List<TimeGrain> timeGrainList = Arrays.asList(timeGrains);
        if (defaultTimeGrain != null) {
            timeGrainList.add(defaultTimeGrain);
        }
        return new DruidMetricName(name, timeGrainList);
    }

    public static DruidMetricName getDruidMetric(String name) {
        List<TimeGrain> timeGrainList = new ArrayList<>();
        if (defaultTimeGrain != null) {
            timeGrainList.add(defaultTimeGrain);
        }
        return new DruidMetricName(name, timeGrainList);
    }

    public static FiliMetricName getFiliMetricName(String name, TimeGrain... timeGrains) {
        List<TimeGrain> timeGrainList = Arrays.asList(timeGrains);
        if (defaultTimeGrain != null) {
            timeGrainList.add(defaultTimeGrain);
        }
        return new FiliMetricName(name, timeGrainList);
    }

    public static FiliMetricName getFiliMetricName(String name) {
        List<TimeGrain> timeGrainList = new ArrayList<>();
        if (defaultTimeGrain != null) {
            timeGrainList.add(defaultTimeGrain);
        }
        return new FiliMetricName(name, timeGrainList);
    }
}
