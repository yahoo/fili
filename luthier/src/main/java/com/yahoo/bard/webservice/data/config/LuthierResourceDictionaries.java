// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config;

import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.table.PhysicalTable;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An extension of resource dictionaries providing for config entities used to supply dependencies.
 */
public class LuthierResourceDictionaries extends ResourceDictionaries {

    private final Map<String, MetricMaker> metricMakerDictionary;

    private final Map<String, SearchProvider> searchProviderDictionary;

    private final Map<String, PhysicalTable> physicalTableDictionary;

    /**
     * Constructor.
     */
    public LuthierResourceDictionaries() {
        super();
        metricMakerDictionary = new LinkedHashMap<>();
        searchProviderDictionary = new LinkedHashMap<>();
        physicalTableDictionary = new LinkedHashMap<>();
    }
    public Map<String, MetricMaker> getMetricMakerDictionary() {
        return metricMakerDictionary;
    }
    public Map<String, SearchProvider> getSearchProviderDictionary() {
        return searchProviderDictionary;
    }
    public Map<String, PhysicalTable> getPhysicalTableDictionary() {
        return physicalTableDictionary;
    }

    /**
     * Supply the default searchProvider dictionaries available in the entire application.
     *
     * @return  A map of named SearchProviders
     */
    public static Map<String, SearchProvider> defaultSearchProviderDictionary() {
        return new HashMap<>();
    }

    /**
     * Supply the default maker dictionaries available in the entire application.
     *
     * @return  A map of named MetricMakers
     */
    public static Map<String, MetricMaker> defaultMakerDictionary() {
        // add Longsum, doublesum, sketchCount, etc
        // plusMaker, minusMaker
        return new HashMap<>();
    }
}
