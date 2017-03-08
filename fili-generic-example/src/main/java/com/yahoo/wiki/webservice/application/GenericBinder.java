// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.application;

import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.wiki.webservice.data.config.auto.ConfigLoader;
import com.yahoo.wiki.webservice.data.config.auto.StaticWikiConfigLoader;
import com.yahoo.wiki.webservice.data.config.dimension.WikiDimensions;
import com.yahoo.wiki.webservice.data.config.metric.WikiMetricLoader;
import com.yahoo.wiki.webservice.data.config.table.WikiTableLoader;

import java.util.LinkedHashSet;

/**
 * Created by kevin on 3/7/2017.
 */
public class GenericBinder {
    private static GenericBinder genericBinder;
    private DruidWebService druidWebService;
    private ConfigLoader configLoader;

    private GenericBinder() {
        configLoader = new StaticWikiConfigLoader();
    }

    public static GenericBinder getInstance() {
        if (genericBinder == null) {
            genericBinder = new GenericBinder();
        }
        return genericBinder;
    }

    public TableLoader buildTableLoader() {
        return new WikiTableLoader(configLoader);
    }

    public LinkedHashSet<DimensionConfig> buildDimensionConfigurations() {
        return new LinkedHashSet<>(new WikiDimensions(configLoader).getAllDimensionConfigurations());
    }

    public MetricLoader buildMetricLoader() {
        return new WikiMetricLoader(configLoader);
    }
}
