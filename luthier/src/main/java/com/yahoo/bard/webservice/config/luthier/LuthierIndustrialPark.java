// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier;

import com.yahoo.bard.webservice.data.config.ConfigurationLoader;
import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.dimension.*;
import com.yahoo.bard.webservice.data.dimension.impl.LuceneSearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.NoOpSearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProvider;
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
    // TODO: Should this really be a package protected method?
    Dimension getDimension(String dimensionName) {
        DimensionDictionary dimensionDictionary = resourceDictionaries.getDimensionDictionary();
        if (dimensionDictionary.findByApiName(dimensionName) == null) {
            Dimension dimension = dimensionFactoryPark.buildEntity(dimensionName, this);
            dimensionDictionary.add(dimension);
        }
        return dimensionDictionary.findByApiName(dimensionName);
    }

    /**
     * Bare minimum that can work
     */

    // TODO: Magic values!
    int MAGIC_queryWeightLimit = 10000;
    String MAGIC_luceneIndexPath = "path";
    int MAGIC_maxResults = 10000;
    public SearchProvider getSearchProvider(String searchProviderName) {
        switch (searchProviderName) {
            case "com.yahoo.bard.webservice.data.dimension.impl.NoOpSearchProvider":
                return new NoOpSearchProvider(MAGIC_queryWeightLimit);
            case "com.yahoo.bard.webservice.data.dimension.impl.LuceneSearchProvider":
                return new LuceneSearchProvider(MAGIC_luceneIndexPath, MAGIC_maxResults);
            default:
                return new ScanSearchProvider();
        }
    }

    public KeyValueStore getKeyValueStore(String keyValueStoreName) throws UnsupportedOperationException {
        switch (keyValueStoreName) {
            // TODO: Magic values!
            case "com.yahoo.bard.webservice.data.dimension.RedisStore":
                throw new UnsupportedOperationException(keyValueStoreName);
            default:
                return new MapStore();
        }
    }


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
        return resourceDictionaries.getPhysicalDictionary();
    }

    @Override
    public ResourceDictionaries getDictionaries() {
        return resourceDictionaries;
    }

    public static class Builder {
    
        private Map<String, Factory<Dimension>> dimensionFactories;
        private final ResourceDictionaries resourceDictionaries;

        /**
         * Constructor.
         *
         * @param resourceDictionaries a class that contains resource dictionaries including
         *                             PhysicalTableDictionary, DimensionDictionary, etc.
         */
        public Builder(ResourceDictionaries resourceDictionaries) {
            this.resourceDictionaries = resourceDictionaries;
            dimensionFactories = getDefaultDimensionFactories();
        }

        /**
         * Constructor.
         * <p>
         * Default to use an empty resource dictionary.
         */
        public Builder() {
            this(new ResourceDictionaries());
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
