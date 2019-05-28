// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config;

import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An extension of resource dictionaries providing for config entities used to supply dependencies.
 */
public class LuthierResourceDictionaries extends ResourceDictionaries {

    private final Map<String, MetricMaker> metricMakerDictionary;

    /**
     * Constructor.
     */
    public LuthierResourceDictionaries() {
        super();
        metricMakerDictionary = new LinkedHashMap<>();
    }
    public Map<String, MetricMaker> getMetricMakerDictionary() {
        return metricMakerDictionary;
    }

    /**
     * Supply the default maker dictionaries available in all application.
     *
     * @return  A map of named MetricMakers
     */
    public static Map<String, MetricMaker> defaultMakerDictionary() {
        // add Longsum, doublesum, sketchCount, etc
        // plusMaker, minusMaker
        return new HashMap<>();
    }
}
