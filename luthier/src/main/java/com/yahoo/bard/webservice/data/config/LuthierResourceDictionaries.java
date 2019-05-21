package com.yahoo.bard.webservice.data.config;

import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;

import java.util.LinkedHashMap;
import java.util.Map;

public class LuthierResourceDictionaries extends ResourceDictionaries {

    public LuthierResourceDictionaries() {
        super();
        metricMakerDictionary = new LinkedHashMap<>();
    }
    public Map<String, MetricMaker> getMetricMakerDictionary() {
        return metricMakerDictionary;
    }

    Map<String, MetricMaker> metricMakerDictionary;



    public static Map<String, MetricMaker> defaultMakerDictionary() {
        // add Longsum, doublesum, sketchCount, etc
        // plusMaker, minusMaker
    }
}
