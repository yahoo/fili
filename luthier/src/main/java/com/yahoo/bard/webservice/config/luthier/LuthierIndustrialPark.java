// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier;

import com.yahoo.bard.webservice.config.luthier.factories.KeyValueStoreDimensionFactory;
import com.yahoo.bard.webservice.config.luthier.factories.LuceneSearchProviderFactory;
import com.yahoo.bard.webservice.config.luthier.factories.NoOpSearchProviderFactory;
import com.yahoo.bard.webservice.config.luthier.factories.ScanSearchProviderFactory;
import com.yahoo.bard.webservice.data.config.ConfigurationLoader;
import com.yahoo.bard.webservice.data.config.LuthierResourceDictionaries;
import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.MapStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.LuceneSearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.NoOpSearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProvider;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Dependency Injection container for Config Objects configured via Luthier.
 */
public class LuthierIndustrialPark implements ConfigurationLoader {

    private final LuthierResourceDictionaries resourceDictionaries;
    private final FactoryPark<Dimension> dimensionFactoryPark;
    private final FactoryPark<SearchProvider> searchProviderFactoryPark;

    /**
     * Constructor.
     *
     * @param resourceDictionaries  The dictionaries to initialize the industrial park with
     * @param dimensionFactories The map of factories for creating dimensions from external config
     * @param searchProviderFactories The map of factories for creating dimensions from external config
     */
    protected LuthierIndustrialPark(
            LuthierResourceDictionaries resourceDictionaries,
            Map<String, Factory<Dimension>> dimensionFactories,
            Map<String, Factory<SearchProvider>> searchProviderFactories
    ) {
        this.resourceDictionaries = resourceDictionaries;
        Supplier<ObjectNode> dimensionConfig = new ResourceNodeSupplier("DimensionConfig.json");
        Supplier<ObjectNode> searchProviderConfig = new ResourceNodeSupplier("SearchProviderConfig.json");
        dimensionFactoryPark = new FactoryPark<>(dimensionConfig, dimensionFactories);
        searchProviderFactoryPark = new FactoryPark<>(searchProviderConfig, searchProviderFactories);
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
    public Dimension getDimension(String dimensionName) {
        DimensionDictionary dimensionDictionary = resourceDictionaries.getDimensionDictionary();
        if (dimensionDictionary.findByApiName(dimensionName) == null) {
            Dimension dimension = dimensionFactoryPark.buildEntity(dimensionName, this);
            dimensionDictionary.add(dimension);
        }
        return dimensionDictionary.findByApiName(dimensionName);
    }

    /**
     * Builds a SearchProvider for a specific domain.
     *
     * @param domain  a string that is associated with the type o
     * @return  an instance of the SearchProvider that correspond to the domain
     */
    public SearchProvider getSearchProvider(String domain) {
        Map<String, SearchProvider> searchProviderDictionary = resourceDictionaries.getSearchProviderDictionary();
        if (! searchProviderDictionary.containsKey(domain)) {
            SearchProvider searchProvider = searchProviderFactoryPark.buildEntity(domain, this);
            searchProviderDictionary.put(domain, searchProvider);
        }
        return searchProviderDictionary.get(domain);
    }

    /**
     * Bare minimum.
     *
     * @param keyValueStoreName identifier of the keyValueStore
     * @return the keyValueStore built according to the keyValueStore identifier
     * @throws UnsupportedOperationException when passed in redisStore.
     */
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

    /**
     * Builder object to construct a new LuthierIndustrialPark instance with.
     */
    public static class Builder {

        private Map<String, Factory<Dimension>> dimensionFactories;
        private Map<String, Factory<SearchProvider>> searchProviderFactories;

        private final LuthierResourceDictionaries resourceDictionaries;

        /**
         * Constructor.
         *
         * @param resourceDictionaries  a class that contains resource dictionaries including
         * PhysicalTableDictionary, DimensionDictionary, etc.
         */
        public Builder(LuthierResourceDictionaries resourceDictionaries) {
            this.resourceDictionaries = resourceDictionaries;
            dimensionFactories = getDefaultDimensionFactories();
            searchProviderFactories = getDefaultSearchProviderFactories();
        }

        /**
         * Constructor.
         * <p>
         * Default to use an empty resource dictionary.
         */
        public Builder() {
            this(new LuthierResourceDictionaries());
        }


        /**
         * Default dimension factories that currently lives in the code base.
         *
         * @return  a LinkedHashMap of KeyValueStoreDimension to its factory
         */
        private Map<String, Factory<Dimension>> getDefaultDimensionFactories() {
            Map<String, Factory<Dimension>> dimensionFactoryMap = new LinkedHashMap<>();
            dimensionFactoryMap.put("KeyValueStoreDimension", new KeyValueStoreDimensionFactory());
            return dimensionFactoryMap;
        }

        /**
         * Default searchProvider factories that currently lives in the code base.
         *
         * @return  a LinkedHashMap of aliases of luceneSearchProvider to its factory
         */
        private Map<String, Factory<SearchProvider>> getDefaultSearchProviderFactories() {
            Map<String, Factory<SearchProvider>> searchProviderFactoryMap = new LinkedHashMap<>();
            // all known factories for searchProviders and their possible aliases
            LuceneSearchProviderFactory luceneSearchProviderFactory = new LuceneSearchProviderFactory();
            List<String> luceneAliases = Arrays.asList(
                    "lucene",
                    LuceneSearchProvider.class.getSimpleName(),
                    LuceneSearchProvider.class.getCanonicalName()
            );
            NoOpSearchProviderFactory noOpSearchProviderFactory = new NoOpSearchProviderFactory();
            List<String> noOpAliases = Arrays.asList(
                    "noOp",
                    NoOpSearchProvider.class.getSimpleName(),
                    NoOpSearchProvider.class.getCanonicalName()
            );
            ScanSearchProviderFactory scanSearchProviderFactory = new ScanSearchProviderFactory();
            List<String> scanAliases = Arrays.asList(
                    "memory",
                    "scan",
                    ScanSearchProvider.class.getSimpleName(),
                    ScanSearchProvider.class.getCanonicalName()
            );
            luceneAliases.forEach(alias -> searchProviderFactoryMap.put(alias, luceneSearchProviderFactory));
            noOpAliases.forEach(alias -> searchProviderFactoryMap.put(alias, noOpSearchProviderFactory));
            scanAliases.forEach(alias -> searchProviderFactoryMap.put(alias, scanSearchProviderFactory));
            return searchProviderFactoryMap;
        }

        /**
         * Registers named dimension factories with the Industrial Park Builder.
         * <p>
         * There should be one factory per type of dimension used in the config
         *
         * @param factories  A mapping from a dimension type identifier used in the config
         * to a factory that builds Dimensions of that type
         *
         * @return the builder object
         */
        public Builder withDimensionFactories(Map<String, Factory<Dimension>> factories) {
            this.dimensionFactories = factories;
            return this;
        }

        /**
         * Registers a named dimension factory with the Industrial Park Builder.
         * <p>
         * There should be one factory per type of dimension used in the config
         *
         * @param name  The identifier used in the configuration to identify the type of
         * dimension built by this factory
         * @param factory  A factory that builds Dimensions of the type named by {@code name}
         *
         * @return the builder object
         */
        public Builder withDimensionFactory(String name, Factory<Dimension> factory) {
            dimensionFactories.put(name, factory);
            return this;
        }

        /**
         * Registers named searchProvider factories with the Industrial Park Builder.
         * <p>
         * There should be one factory per type of searchProvider used in the config
         *
         * @param factories  A mapping from a searchProvider type identifier used in the config
         * to a factory that builds SearchProvider of that type
         *
         * @return the builder object
         */
        public Builder withSearchProviderFactories(Map<String, Factory<SearchProvider>> factories) {
            this.searchProviderFactories = factories;
            return this;
        }

        /**
         * Registers a named searchProvider factory with the Industrial Park Builder.
         * <p>
         * There should be one factory per type of searchProvider used in the config
         *
         * @param name  The identifier used in the configuration to identify the type of
         * searchProvider built by this factory
         * @param factory  A factory that builds searchProvider of the type named by {@code name}
         *
         * @return the builder object
         */
        public Builder withSearchProviderFactory(String name, Factory<SearchProvider> factory) {
            searchProviderFactories.put(name, factory);
            return this;
        }

        /**
         * Builds a LuthierIndustrialPark.
         *
         * @return the LuthierIndustrialPark with the specified resourceDictionaries and factories
         */
        public LuthierIndustrialPark build() {
            return new LuthierIndustrialPark(
                    resourceDictionaries,
                    new LinkedHashMap<>(dimensionFactories),
                    new LinkedHashMap<>(searchProviderFactories)
            );
        }
    }
}
