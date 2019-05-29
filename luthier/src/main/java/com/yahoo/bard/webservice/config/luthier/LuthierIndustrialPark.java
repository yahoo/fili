// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier;

import com.yahoo.bard.webservice.data.config.ConfigurationLoader;
import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Dependency Injection container for Config Objects configured via Luthier.
 */
public class LuthierIndustrialPark implements ConfigurationLoader {

    private final ResourceDictionaries resourceDictionaries;

    private final Map<String, Factory<Dimension>> dimensionFactories;

    private final FactoryPark<Dimension> dimensionFactoryPark;

    /**
     * Constructor.
     *
     * @param resourceDictionaries  The dictionaries to initialize the industrial park with.
     * @param dimensionFactories The map of factories for creating dimensions from external config
     */
    protected LuthierIndustrialPark(
            ResourceDictionaries resourceDictionaries,
            Map<String, Factory<Dimension>> dimensionFactories
    ) {
        this.resourceDictionaries = resourceDictionaries;
        this.dimensionFactories = dimensionFactories;
        Supplier<ObjectNode> dimensionConfig = new ResourceNodeSupplier("DimensionConfig.json");
        dimensionFactoryPark = new FactoryPark<>(dimensionConfig, dimensionFactories);
    }

/*
    LogicalTable getLogicalTable(String tableName);
    PhysicalTable getPhysicalTable(String tableName);
    LogicalMetric getLogicalMetric(String metricName);
    MetricMaker getMetricMaker(String makerName);
*/

    /**
     * Retrieve or build a dimension.
     *
     * @param dimensionName the name for the dimension to be provided.
     *
     * @return the dimension instance corresponding to this name.
     */
    Dimension getDimension(String dimensionName) {
        DimensionDictionary dimensionDictionary = resourceDictionaries.getDimensionDictionary();
        if (dimensionDictionary.findByApiName(dimensionName) == null) {
            Dimension dimension = dimensionFactoryPark.buildEntity(dimensionName, this);
            dimensionDictionary.add(dimension);
        }
        return dimensionDictionary.findByApiName(dimensionName);
    }

/*
    SearchProvider getSearchProvider(String searchProviderName);
    KeyValueStore getKeyValueStore(String keyValueStoreName);
*/

    @Override
    public void load() {
        dimensionFactoryPark.fetchConfig().fieldNames().forEachRemaining(this::getDimension);
    }

    @Override
    public DimensionDictionary getDimensionDictionary() {
        return resourceDictionaries.getDimensionDictionary();
    }

    @Override
    public MetricDictionary getMetricDictionary() {
        return resourceDictionaries.getMetricDictionary();
    }

    @Override
    public LogicalTableDictionary getLogicalTableDictionary() {
        return resourceDictionaries.getLogicalDictionary();
    }

    @Override
    public PhysicalTableDictionary getPhysicalTableDictionary() {
        return getPhysicalTableDictionary();
    }

    @Override
    public ResourceDictionaries getDictionaries() {
        return resourceDictionaries;
    }

    public class Builder {
    
        private Map<String, Factory<Dimension>> dimensionFactories;
        private final ResourceDictionaries resourceDictionaries;
        public Builder(ResourceDictionaries resourceDictionaries) {
            this.resourceDictionaries = resourceDictionaries;
            dimensionFactories = getDefaultDimensionFactories();
        }

        public Map<String, Factory<Dimension>> getDefaultDimensionFactories() {
            return new LinkedHashMap<>();
        }

        public Builder withDimensionFactories(Map<String, Factory<Dimension>> factories) {
            this.dimensionFactories = factories;
            return this;
        }

        public Builder withDimensionFactory(String name, Factory<Dimension> factory) {
            dimensionFactories.put(name, factory);
            return this;
        }

        public LuthierIndustrialPark build() {
            return new LuthierIndustrialPark(resourceDictionaries, new LinkedHashMap<>(dimensionFactories));
        }
    } 
}
