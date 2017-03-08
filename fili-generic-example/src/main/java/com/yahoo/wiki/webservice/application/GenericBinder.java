// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.application;

import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.wiki.webservice.data.config.auto.ConfigLoader;
import com.yahoo.wiki.webservice.data.config.auto.StaticWikiConfigLoader;
import com.yahoo.wiki.webservice.data.config.dimension.GenericDimensions;
import com.yahoo.wiki.webservice.data.config.metric.GenericMetricLoader;
import com.yahoo.wiki.webservice.data.config.table.GenericTableLoader;

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
        //druidWebService = null; //TODO how to initialize with actual DruidWebService
        //configLoader = new DruidNavigator(druidWebService);
    }

    public static GenericBinder getInstance() {
        if (genericBinder == null) {
            genericBinder = new GenericBinder();
        }
        return genericBinder;
    }

    public TableLoader buildTableLoader() {
        return new GenericTableLoader(configLoader);
    }

    public LinkedHashSet<DimensionConfig> buildDimensionConfigurations() {
        return new LinkedHashSet<>(new GenericDimensions(configLoader).getAllDimensionConfigurations());
    }

    public MetricLoader buildMetricLoader() {
        return new GenericMetricLoader(configLoader);
    }
}
