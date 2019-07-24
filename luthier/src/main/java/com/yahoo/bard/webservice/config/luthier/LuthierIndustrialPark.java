// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier;

import com.yahoo.bard.webservice.config.luthier.factories.DefaultLogicalTableGroupFactory;
import com.yahoo.bard.webservice.config.luthier.factories.KeyValueStoreDimensionFactory;
import com.yahoo.bard.webservice.config.luthier.factories.LuceneSearchProviderFactory;
import com.yahoo.bard.webservice.config.luthier.factories.MapKeyValueStoreFactory;
import com.yahoo.bard.webservice.config.luthier.factories.NoOpSearchProviderFactory;
import com.yahoo.bard.webservice.config.luthier.factories.PermissivePhysicalTableFactory;
import com.yahoo.bard.webservice.config.luthier.factories.ScanSearchProviderFactory;
import com.yahoo.bard.webservice.config.luthier.factories.StrictPhysicalTableFactory;
import com.yahoo.bard.webservice.data.config.ConfigurationLoader;
import com.yahoo.bard.webservice.data.config.LogicalTableGroup;
import com.yahoo.bard.webservice.data.config.LuthierResourceDictionaries;
import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.LuceneSearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.NoOpSearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProvider;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.GranularityDictionary;
import com.yahoo.bard.webservice.data.time.GranularityParser;
import com.yahoo.bard.webservice.data.time.StandardGranularityParser;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.bard.webservice.table.TableIdentifier;

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
    private final GranularityDictionary granularityDictionary;
    private final FactoryPark<Dimension> dimensionFactoryPark;
    private final FactoryPark<SearchProvider> searchProviderFactoryPark;
    private final FactoryPark<ConfigPhysicalTable> physicalTableFactoryPark;
    private final FactoryPark<KeyValueStore> keyValueStoreFactoryPark;
    private final FactoryPark<LogicalTableGroup> logicalTableGroupFactoryPark;
    // all physical tables should query this metadataService to get their availability
    private final DataSourceMetadataService metadataService;
    private final GranularityParser granularityParser;

    /**
     * Constructor.
     *
     * @param resourceDictionaries  The dictionaries to initialize the industrial park with
     * @param granularityDictionary  The named map of granularities to configure tables
     * @param dimensionFactories  The map of factories for creating dimensions from external config
     * @param keyValueStoreFactories  The map of factories for creating keyValueStores from external config
     * @param searchProviderFactories  The map of factories for creating searchProviders from external config
     * @param physicalTableFactories  The map of factories for creating physicalTables from external config
     * @param logicalTableGroupFactories  The map of factories for creating logicalTables from external config
     */
    protected LuthierIndustrialPark(
            LuthierResourceDictionaries resourceDictionaries,
            GranularityDictionary granularityDictionary,
            Map<String, Factory<Dimension>> dimensionFactories,
            Map<String, Factory<KeyValueStore>> keyValueStoreFactories,
            Map<String, Factory<SearchProvider>> searchProviderFactories,
            Map<String, Factory<ConfigPhysicalTable>> physicalTableFactories,
            Map<String, Factory<LogicalTableGroup>> logicalTableGroupFactories
    ) {
        this.resourceDictionaries = resourceDictionaries;
        this.granularityDictionary = granularityDictionary;
        this.granularityParser = new StandardGranularityParser(granularityDictionary);
        Supplier<ObjectNode> dimensionConfig = new ResourceNodeSupplier("DimensionConfig.json");
        Supplier<ObjectNode> keyValueStoreConfig = new ResourceNodeSupplier("KeyValueStoreConfig.json");
        Supplier<ObjectNode> searchProviderConfig = new ResourceNodeSupplier("SearchProviderConfig.json");
        Supplier<ObjectNode> physicalTableConfig = new ResourceNodeSupplier("PhysicalTableConfig.json");
        Supplier<ObjectNode> logicalTableConfig = new ResourceNodeSupplier("LogicalTableConfig.json");
        this.dimensionFactoryPark = new FactoryPark<>(dimensionConfig, dimensionFactories);
        this.keyValueStoreFactoryPark = new FactoryPark<>(keyValueStoreConfig, keyValueStoreFactories);
        this.searchProviderFactoryPark = new FactoryPark<>(searchProviderConfig, searchProviderFactories);
        this.physicalTableFactoryPark = new FactoryPark<>(physicalTableConfig, physicalTableFactories);
        this.logicalTableGroupFactoryPark = new FactoryPark<>(logicalTableConfig, logicalTableGroupFactories);
        this.metadataService = new DataSourceMetadataService();
    }

/*
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
     * Retrieve or build a SearchProvider for a specific domain.
     *
     * @param domain  a string that is associated with the space this provider
     * searches for. It will typically be the dimension name unless more than
     * one dimension shares the same SearchProvider.
     *
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
     * @param domain identifier of the keyValueStore
     *
     * @return the keyValueStore built according to the keyValueStore identifier
     * @throws UnsupportedOperationException when passed in redisStore.
     */
    public KeyValueStore getKeyValueStore(String domain) throws UnsupportedOperationException {
        Map<String, KeyValueStore> keyValueStoreDictionary = resourceDictionaries.getKeyValueStoreDictionary();
        if (! keyValueStoreDictionary.containsKey(domain)) {
            KeyValueStore keyValueStore = keyValueStoreFactoryPark.buildEntity(domain, this);
            keyValueStoreDictionary.put(domain, keyValueStore);
        }
        return keyValueStoreDictionary.get(domain);
    }

    /**
     * Retrieve or build a PhysicalTable.
     *
     * @param tableName the name for the PhysicalTable to be retrieved or built.
     *
     * @return the PhysicalTable instance corresponding to this name.
     */
    public ConfigPhysicalTable getPhysicalTable(String tableName) {
        PhysicalTableDictionary physicalTableDictionary = resourceDictionaries.getPhysicalDictionary();
        if (! physicalTableDictionary.containsKey(tableName)) {
            ConfigPhysicalTable physicalTable = physicalTableFactoryPark.buildEntity(tableName, this);
            physicalTableDictionary.put(tableName, physicalTable);
        }
        return physicalTableDictionary.get(tableName);
    }

    public DataSourceMetadataService getMetadataService() {
        return metadataService;
    }


    public GranularityParser getGranularityParser() {
        return granularityParser;
    }

    /**
     * Retrieve or build a metric.
     *
     * @param metricName  the name pf the LogicalMetric to be retrieved or built.
     *
     * @return the LogicalMetric instance corresponding to this metricName.
     */
    public LogicalMetric getMetric(String metricName) {
        // TODO: to be finished in the metric PR.
        return null;
    }

    /**
     * Retrieve or build a LogicalTable.
     *
     * @param tableId the identifier for table name and granularity to be retrieved or built
     *
     * @return the logicalTable instance corresponding to this TableIdentifier.
     */
    public LogicalTable getLogicalTable(TableIdentifier tableId) {
        LogicalTableDictionary logicalTableDictionary = resourceDictionaries.getLogicalDictionary();
        if (! logicalTableDictionary.containsKey(tableId)) {
            LogicalTableGroup logicalTableGroup = logicalTableGroupFactoryPark.buildEntity(tableId.getKey(), this);
            logicalTableDictionary.putAll(logicalTableGroup);
        }
        return logicalTableDictionary.get(tableId);
    }

    /**
     * Private setters to put a group of LogicalTable's.
     *
     * @param tableKey named tableKey according to the definition in the json Config
     */
    private void putLogicalTableByGroup(String tableKey) {
        LogicalTableDictionary logicalTableDictionary = resourceDictionaries.getLogicalDictionary();
        LogicalTableGroup logicalTableGroup = logicalTableGroupFactoryPark.buildEntity(tableKey, this);
        logicalTableDictionary.putAll(logicalTableGroup);
    }

    @Override
    public void load() {
        logicalTableGroupFactoryPark.fetchConfig().fieldNames().forEachRemaining(this::putLogicalTableByGroup);
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

    public GranularityDictionary getGranularityDictionary() {
        return granularityDictionary;
    }

    /**
     * Builder object to construct a new LuthierIndustrialPark instance with.
     */
    public static class Builder {

        private Map<String, Factory<Dimension>> dimensionFactories;
        private Map<String, Factory<SearchProvider>> searchProviderFactories;
        private Map<String, Factory<KeyValueStore>> keyValueStoreFactories;
        private Map<String, Factory<ConfigPhysicalTable>> physicalTableFactories;
        private Map<String, Factory<LogicalTableGroup>> logicalTableGroupFactories;
        private GranularityDictionary granularityDictionary;
        private final LuthierResourceDictionaries resourceDictionaries;

        /**
         * Constructor.
         *
         * @param resourceDictionaries  a class that contains resource dictionaries including
         * PhysicalTableDictionary, DimensionDictionary, etc.
         */
        public Builder(LuthierResourceDictionaries resourceDictionaries) {
            this(
                    resourceDictionaries,
                    StandardGranularityParser.getDefaultGrainMap()
            );
        }

        /**
         * Constructor.
         *
         * @param resourceDictionaries  a class that contains resource dictionaries including
         * PhysicalTableDictionary, DimensionDictionary, etc.
         * @param granularityDictionary  a named map of all supported granularities
         */
        public Builder(LuthierResourceDictionaries resourceDictionaries, GranularityDictionary granularityDictionary) {
            this.resourceDictionaries = resourceDictionaries;
            this.granularityDictionary = granularityDictionary;
            dimensionFactories = getDefaultDimensionFactories();
            keyValueStoreFactories = getDefaultKeyValueStoreFactories();
            searchProviderFactories = getDefaultSearchProviderFactories();
            physicalTableFactories = getDefaultPhysicalTableFactories();
            logicalTableGroupFactories = getDefaultLogicalTableGroupFactories();
        }

        /**
         * Default dimension factories that are defined in fili-core.
         *
         * @return  a LinkedHashMap of KeyValueStoreDimension to its factory
         */
        private Map<String, Factory<Dimension>> getDefaultDimensionFactories() {
            Map<String, Factory<Dimension>> dimensionFactoryMap = new LinkedHashMap<>();
            dimensionFactoryMap.put("KeyValueStoreDimension", new KeyValueStoreDimensionFactory());
            return dimensionFactoryMap;
        }

        /**
         * Default keyValueStore factories that are defined in fili-core.
         *
         * @return  a LinkedHashMap of KeyValueStore to its factory
         */
        private Map<String, Factory<KeyValueStore>> getDefaultKeyValueStoreFactories() {
            Map<String, Factory<KeyValueStore>> keyValueStoreFactoryMap = new LinkedHashMap<>();
            MapKeyValueStoreFactory mapStoreFactory = new MapKeyValueStoreFactory();
            keyValueStoreFactoryMap.put("memory", mapStoreFactory);
            keyValueStoreFactoryMap.put("map", mapStoreFactory);
            keyValueStoreFactoryMap.put("mapStore", mapStoreFactory);
            keyValueStoreFactoryMap.put("com.yahoo.bard.webservice.data.dimension.MapStore", mapStoreFactory);
            // TODO: add in Redis Store later
            return keyValueStoreFactoryMap;
        }

        /**
         * Default searchProvider factories that are defined in fili-core.
         *
         * @return  a LinkedHashMap of aliases of search provider type name to its factory
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
         * Default PhysicalTable factories that are defined in fili-core.
         *
         * @return  a LinkedHashMap of physicalTable type name to its factory
         */
        private Map<String, Factory<ConfigPhysicalTable>> getDefaultPhysicalTableFactories() {
            Map<String, Factory<ConfigPhysicalTable>> physicalTableFactoryMap = new LinkedHashMap<>();
            StrictPhysicalTableFactory strictFactory = new StrictPhysicalTableFactory();
            PermissivePhysicalTableFactory permissiveFactory = new PermissivePhysicalTableFactory();
            physicalTableFactoryMap.put("strictPhysicalTable", strictFactory);
            physicalTableFactoryMap.put("strict", strictFactory);
            physicalTableFactoryMap.put("permissivePhysicalTable", permissiveFactory);
            physicalTableFactoryMap.put("permissive", permissiveFactory);
            return physicalTableFactoryMap;
        }

        /**
         * Default LogicalTable factories that are defined in fili-core.
         *
         * @return  a LinkedHashMap of logical Table type name to its factory
         */
        private Map<String, Factory<LogicalTableGroup>> getDefaultLogicalTableGroupFactories() {
            Map<String, Factory<LogicalTableGroup>> logicalTableFactoryMap = new LinkedHashMap<>();
            DefaultLogicalTableGroupFactory defaultFactory = new DefaultLogicalTableGroupFactory();
            logicalTableFactoryMap.put("default", defaultFactory);
            logicalTableFactoryMap.put("DefaultLogicalTableGroup", defaultFactory);
            return logicalTableFactoryMap;
        }

        public GranularityDictionary getGranularityDictionary() {
            return granularityDictionary;
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
         * Registers named KeyValueStore factories with the Industrial Park Builder.
         * <p>
         * There should be one factory per type of keyValueStore used in the config
         *
         * @param factories  A mapping from a keyValueStore type identifier used in the config
         * to a factory that builds keyValueStore of that type
         *
         * @return the builder object
         */
        public Builder withKeyValueStoreFactories(Map<String, Factory<KeyValueStore>> factories) {
            this.keyValueStoreFactories = factories;
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
         * Registers named PhysicalTable factories with the Industrial Park Builder.
         * <p>
         * There should be one factory per type of physicalTable used in the config
         *
         * @param factories  A mapping from a PhysicalTable type identifier used in the config
         * to a factory that builds PhysicalTable of that type
         *
         * @return the builder object
         */
        public Builder withPhysicalTableFactories(Map<String, Factory<ConfigPhysicalTable>> factories) {
            this.physicalTableFactories = factories;
            return this;
        }

        /**
         * Register one named Granularity into the GranularityDictionary.
         *
         * @param name  the identifier of the new granularity as a String
         * @param granularity  the new added granularity
         */
        public void addGranularity(String name, Granularity granularity) {
            this.granularityDictionary.put(name, granularity);
        }

        public void setGranularityDictionary(GranularityDictionary granularityDictionary) {
            this.granularityDictionary = granularityDictionary;
        }

        /**
         * Registers named custom GranularityDictionary if new granularity type needs to be supported.
         * <p>
         * Will replace the original GranularityDictionary
         *
         * @param granularityDictionary  maps granularity name (String) to granularity instance
         *
         * @return the builder object
         */
        public Builder withGranularityDictionary(GranularityDictionary granularityDictionary) {
            this.granularityDictionary = granularityDictionary;
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
                    granularityDictionary,
                    new LinkedHashMap<>(dimensionFactories),
                    new LinkedHashMap<>(keyValueStoreFactories),
                    new LinkedHashMap<>(searchProviderFactories),
                    new LinkedHashMap<>(physicalTableFactories),
                    new LinkedHashMap<>(logicalTableGroupFactories)
            );
        }
    }
}
