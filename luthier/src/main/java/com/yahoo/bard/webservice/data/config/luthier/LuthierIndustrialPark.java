// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier;

import com.yahoo.bard.webservice.data.config.ConfigurationLoader;
import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.luthier.factories.metricmaker.AggregationAverageMakerFactory;
import com.yahoo.bard.webservice.data.config.luthier.factories.metricmaker.ArithmeticMakerFactory;
import com.yahoo.bard.webservice.data.config.luthier.factories.metricmaker.CountMakerFactory;
import com.yahoo.bard.webservice.data.config.luthier.factories.logicaltable.DefaultLogicalTableGroupFactory;
import com.yahoo.bard.webservice.data.config.luthier.factories.logicalmetric.DefaultMetricFactory;
import com.yahoo.bard.webservice.data.config.luthier.factories.metricmaker.DoubleMaxMakerFactory;
import com.yahoo.bard.webservice.data.config.luthier.factories.metricmaker.DoubleMinMakerFactory;
import com.yahoo.bard.webservice.data.config.luthier.factories.metricmaker.DoubleSumMakerFactory;
import com.yahoo.bard.webservice.data.config.luthier.factories.dimension.KeyValueStoreDimensionFactory;
import com.yahoo.bard.webservice.data.config.luthier.factories.metricmaker.LongMaxMakerFactory;
import com.yahoo.bard.webservice.data.config.luthier.factories.metricmaker.LongMinMakerFactory;
import com.yahoo.bard.webservice.data.config.luthier.factories.metricmaker.LongSumMakerFactory;
import com.yahoo.bard.webservice.data.config.luthier.factories.searchprovider.LuceneSearchProviderFactory;
import com.yahoo.bard.webservice.data.config.luthier.factories.keyvaluestore.MapKeyValueStoreFactory;
import com.yahoo.bard.webservice.data.config.luthier.factories.searchprovider.NoOpSearchProviderFactory;
import com.yahoo.bard.webservice.data.config.luthier.factories.physicaltable.PermissivePhysicalTableFactory;
import com.yahoo.bard.webservice.data.config.luthier.factories.searchprovider.ScanSearchProviderFactory;
import com.yahoo.bard.webservice.data.config.luthier.factories.physicaltable.StrictPhysicalTableFactory;
import com.yahoo.bard.webservice.data.config.luthier.table.LogicalTableGroup;
import com.yahoo.bard.webservice.data.config.metric.makers.AggregationAverageMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.ArithmeticMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.CountMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.DoubleMaxMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.DoubleMinMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.DoubleSumMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.LongMaxMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.LongMinMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.LongSumMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;
import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.MapStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension;
import com.yahoo.bard.webservice.data.dimension.impl.LuceneSearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.NoOpSearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProvider;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.GranularityDictionary;
import com.yahoo.bard.webservice.data.time.GranularityParser;
import com.yahoo.bard.webservice.data.time.StandardGranularityParser;
import com.yahoo.bard.webservice.metadata.DataSourceMetadata;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.PermissivePhysicalTable;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.bard.webservice.table.StrictPhysicalTable;
import com.yahoo.bard.webservice.table.TableIdentifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.validation.constraints.NotNull;

/**
 * Dependency Injection container for Config Objects configured via Luthier.
 */
public class LuthierIndustrialPark implements ConfigurationLoader {

    private final LuthierResourceDictionaries resourceDictionaries;

    private final GranularityParser granularityParser;

    // all physical tables should query this metadataService to get their availability
    private final DataSourceMetadataService metadataService;

    private final FactoryPark<Dimension> dimensionFactoryPark;
    private final FactoryPark<SearchProvider> searchProviderFactoryPark;
    private final FactoryPark<KeyValueStore> keyValueStoreFactoryPark;

    private final FactoryPark<LogicalMetric> metricFactoryPark;
    private final FactoryPark<MetricMaker> metricMakerFactoryPark;

    private final FactoryPark<ConfigPhysicalTable> physicalTableFactoryPark;
    private final FactoryPark<LogicalTableGroup> logicalTableGroupFactoryPark;


    /**
     * Constructor.
     *
     * @param resourceDictionaries  The dictionaries to initialize the industrial park with
     * @param granularityDictionary  The dictionary of granularities supported by entities on this system
     * @param conceptFactoryMap  A collection of named factories, partitioned by concept types
     */
    protected LuthierIndustrialPark(
            LuthierResourceDictionaries resourceDictionaries,
            @NotNull GranularityDictionary granularityDictionary,
            Map<ConceptType<?>, Map<String, ? extends Factory<? extends Object>>> conceptFactoryMap
    ) {

        this.resourceDictionaries = resourceDictionaries;
        this.granularityParser = new StandardGranularityParser(granularityDictionary);

        this.searchProviderFactoryPark = buildFactoryPark(ConceptType.SEARCH_PROVIDER, conceptFactoryMap);
        this.keyValueStoreFactoryPark = buildFactoryPark(ConceptType.KEY_VALUE_STORE, conceptFactoryMap);
        this.dimensionFactoryPark = buildFactoryPark(ConceptType.DIMENSION, conceptFactoryMap);

        this.metricFactoryPark =  buildFactoryPark(ConceptType.METRIC, conceptFactoryMap);
        this.metricMakerFactoryPark =  buildFactoryPark(ConceptType.METRIC_MAKER, conceptFactoryMap);

        this.physicalTableFactoryPark = buildFactoryPark(ConceptType.PHYSICAL_TABLE, conceptFactoryMap);
        this.logicalTableGroupFactoryPark = buildFactoryPark(ConceptType.LOGICAL_TABLE_GROUP, conceptFactoryMap);

        this.metadataService = new DataSourceMetadataService();
    }

    /**
     * For a given collection of factories, build a single FactoryPark.
     *
     * @param concept  The concept of the factory map being created.
     * @param conceptFactoryMap  A collection of named factories, partitioned by concept types
     * @param <T> The type of the entity corresponding to this factory map.
     *
     * @return  A Factory Park defined for a given set of named factories.
     */
    private static <T> FactoryPark<T> buildFactoryPark(
            ConceptType<T> concept,
            Map<ConceptType<?>, Map<String, ? extends Factory<? extends Object>>> conceptFactoryMap
    ) {
        Map<String, Factory<T>> factories = (Map<String, Factory<T>>) conceptFactoryMap.get(concept);

        return new FactoryPark<>(
                new LuaJNodeSupplier(concept.getResourceName()),
                factories
        );
    }

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
            Dimension dimension = dimensionFactoryPark.buildEntity(dimensionName, this, ConceptType.DIMENSION);
            dimensionDictionary.add(dimension);
        }
        return dimensionDictionary.findByApiName(dimensionName);
    }

    /**
     * Retrieve or build a SearchProvider.
     *
     * @param domain  a string that is associated with the space this provider
     * searches for. It will typically be the dimension name unless more than
     * one dimension shares the same SearchProvider.
     *
     * @return an instance of the SearchProvider that correspond to the domain
     */
    public SearchProvider getSearchProvider(String domain) {
        Map<String, SearchProvider> searchProviderDictionary = resourceDictionaries.getSearchProviderDictionary();
        if (!searchProviderDictionary.containsKey(domain)) {
            SearchProvider searchProvider = searchProviderFactoryPark.buildEntity(
                    domain,
                    this,
                    ConceptType.SEARCH_PROVIDER
            );
            searchProviderDictionary.put(domain, searchProvider);
        }
        return searchProviderDictionary.get(domain);
    }

    /**
     * Retrieve or build a KeyValueStore.
     *
     * @param domain identifier of the keyValueStore
     *
     * @return the keyValueStore built according to the keyValueStore identifier
\     */
    public KeyValueStore getKeyValueStore(String domain) {
        Map<String, KeyValueStore> keyValueStoreDictionary = resourceDictionaries.getKeyValueStoreDictionary();
        if (! keyValueStoreDictionary.containsKey(domain)) {
            KeyValueStore keyValueStore = keyValueStoreFactoryPark.buildEntity(
                    domain,
                    this,
                    ConceptType.KEY_VALUE_STORE
            );
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
        if (!physicalTableDictionary.containsKey(tableName)) {
            ConfigPhysicalTable physicalTable = physicalTableFactoryPark.buildEntity(
                    tableName,
                    this,
                    ConceptType.PHYSICAL_TABLE
            );
            physicalTableDictionary.put(tableName, physicalTable);
            // registers each table we build in the metadataService
            metadataService.update(
                    DataSourceName.of(tableName),
                    new DataSourceMetadata(
                            tableName,
                            Collections.emptyMap(),
                            Collections.emptyList()
                    )
            );
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
        Map<String, LogicalMetric> metricDictionary = resourceDictionaries.getMetricDictionary();
        if (metricDictionary.get(metricName) == null) {
            LogicalMetric metric = metricFactoryPark.buildEntity(metricName, this, ConceptType.METRIC);
            metricDictionary.put(metricName, metric);
        }
        return metricDictionary.get(metricName);
    }

    /**
     * Retrieve or build a Metric Maker.
     *
     * @param metricMakerName the name for the metric maker to be provided.
     *
     * @return the dimension instance corresponding to this name.
     */
    public MetricMaker getMetricMaker(String metricMakerName) {
        Map<String, MetricMaker> metricMakerDictionary = resourceDictionaries.getMetricMakerDictionary();
        if (metricMakerDictionary.get(metricMakerName) == null) {
            MetricMaker metricMaker = metricMakerFactoryPark.buildEntity(
                    metricMakerName,
                    this,
                    ConceptType.METRIC_MAKER
            );
            metricMakerDictionary.put(metricMakerName, metricMaker);
        }
        return metricMakerDictionary.get(metricMakerName);
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
            LogicalTableGroup logicalTableGroup = logicalTableGroupFactoryPark.buildEntity(
                    tableId.getKey(),
                    this,
                    ConceptType.LOGICAL_TABLE_GROUP
            );
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
        LogicalTableGroup logicalTableGroup = logicalTableGroupFactoryPark.buildEntity(
                tableKey,
                this,
                ConceptType.LOGICAL_TABLE_GROUP
        );
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

    /**
     * Builder object to construct a new LuthierIndustrialPark instance with.
     */
    public static class Builder {

        private Map<ConceptType<?>, Map<String, ? extends Factory<? extends Object>>> conceptFactoryMap;
        private GranularityDictionary granularityDictionary;

        private final LuthierResourceDictionaries resourceDictionaries;

        /**
         * Constructor.
         */
        public Builder() {
            this(new LuthierResourceDictionaries(), StandardGranularityParser.getDefaultGrainMap());
        }

        /**
         * Constructor.
         *
         * @param resourceDictionaries  a class that contains resource dictionaries including
         * PhysicalTableDictionary, DimensionDictionary, etc.
         */
        public Builder(LuthierResourceDictionaries resourceDictionaries) {
            this(resourceDictionaries, StandardGranularityParser.getDefaultGrainMap());
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

            conceptFactoryMap = new HashMap<>();
            conceptFactoryMap.put(ConceptType.DIMENSION, getDefaultDimensionFactories());
            conceptFactoryMap.put(ConceptType.SEARCH_PROVIDER, getDefaultSearchProviderFactories());
            conceptFactoryMap.put(ConceptType.METRIC_MAKER, getDefaultMetricMakerFactories());
            conceptFactoryMap.put(ConceptType.METRIC, getDefaultMetricFactories());
            conceptFactoryMap.put(ConceptType.PHYSICAL_TABLE, getDefaultPhysicalTableFactories());
            conceptFactoryMap.put(ConceptType.KEY_VALUE_STORE, getDefaultKeyValueStoreFactories());
            conceptFactoryMap.put(ConceptType.LOGICAL_TABLE_GROUP, getDefaultLogicalTableGroupFactories());
        }

        /**
         * Add the canonical name, simple name, and any specified aliases of a Factory to the factory map.
         *
         * @param factoryMap  the factory map we will populate
         * @param factory  the factory to be associated
         * @param conceptClass  the specific class we use to extract canonical class name and simple class name from
         * @param aliases  the list of custom alias we want to add to the factory map
         * @param <T>  Factory generic type
         */
        private <T> void addAliasesToFactory(
                Map<String, Factory<T>> factoryMap,
                Factory<T> factory,
                Class<? extends T> conceptClass,
                String ... aliases
        ) {
            String canonicalName = conceptClass.getCanonicalName();
            String simpleName = conceptClass.getSimpleName();
            Arrays.stream(aliases).forEach(s -> factoryMap.put(s, factory));
            factoryMap.put(canonicalName, factory);
            factoryMap.put(simpleName, factory);
        }

        /**
         * Default dimension factories that are defined in fili-core.
         *
         * @return a Map of KeyValueStoreDimension to its factory
         */
        private Map<String, Factory<Dimension>> getDefaultDimensionFactories() {
            Map<String, Factory<Dimension>> factoryMap = new LinkedHashMap<>();
            addAliasesToFactory(factoryMap, new KeyValueStoreDimensionFactory(), KeyValueStoreDimension.class);
            return factoryMap;
        }

        /**
         * Default keyValueStore factories that are defined in fili-core.
         *
         * @return  a Map of KeyValueStore to its factory
         */
        private Map<String, Factory<KeyValueStore>> getDefaultKeyValueStoreFactories() {
            Map<String, Factory<KeyValueStore>> factoryMap = new LinkedHashMap<>();
            addAliasesToFactory(factoryMap, new MapKeyValueStoreFactory(), MapStore.class, "memory", "map");
            // TODO: add in Redis Store later
            return factoryMap;
        }

        /**
         * Default searchProvider factories that are defined in fili-core.
         *
         * @return a Map of aliases of search provider type name to its factory
         */
        private Map<String, Factory<SearchProvider>> getDefaultSearchProviderFactories() {
            Map<String, Factory<SearchProvider>> factoryMap = new LinkedHashMap<>();
            // all known factories for searchProviders and their possible aliases
            LuceneSearchProviderFactory luceneFactory = new LuceneSearchProviderFactory();
            NoOpSearchProviderFactory noOpFactory = new NoOpSearchProviderFactory();
            ScanSearchProviderFactory scanFactory = new ScanSearchProviderFactory();

            addAliasesToFactory(factoryMap, luceneFactory, LuceneSearchProvider.class, "lucene");
            addAliasesToFactory(factoryMap, noOpFactory, NoOpSearchProvider.class, "noOp");
            addAliasesToFactory(factoryMap, scanFactory, ScanSearchProvider.class, "memory", "scan");
            return factoryMap;
        }

        /**
         * Default PhysicalTable factories that are defined in fili-core.
         *
         * @return a Map of physicalTable type name to its factory
         */
        private Map<String, Factory<ConfigPhysicalTable>> getDefaultPhysicalTableFactories() {
            Map<String, Factory<ConfigPhysicalTable>> physicalTableFactoryMap = new LinkedHashMap<>();
            addAliasesToFactory(
                    physicalTableFactoryMap,
                    new StrictPhysicalTableFactory(),
                    StrictPhysicalTable.class,
                    "strict"
            );
            addAliasesToFactory(
                    physicalTableFactoryMap,
                    new PermissivePhysicalTableFactory(),
                    PermissivePhysicalTable.class,
                    "permissive"
            );
            return physicalTableFactoryMap;
        }

        /**
         * Default LogicalTableGroup factories that are defined in fili-core.
         *
         * @return  a Map of LogicalTableGroup type name to its factory
         */
        private Map<String, Factory<LogicalTableGroup>> getDefaultLogicalTableGroupFactories() {
            Map<String, Factory<LogicalTableGroup>> logicalTableFactoryMap = new LinkedHashMap<>();
            addAliasesToFactory(
                    logicalTableFactoryMap,
                    new DefaultLogicalTableGroupFactory(),
                    LogicalTableGroup.class,
                    "default"
            );
            return logicalTableFactoryMap;
        }

        /**
         * Default LogicalTable factories that are defined in fili-core.
         *
         * @return  a Map of MetricMaker name to its factory
         */
        private Map<String, Factory<LogicalMetric>> getDefaultMetricFactories() {
            Map<String, Factory<LogicalMetric>> metricFactoryMap = new LinkedHashMap<>();
            addAliasesToFactory(
                    metricFactoryMap,
                    new DefaultMetricFactory(),
                    LogicalMetric.class,
                    "default"
            );
            return metricFactoryMap;
        }

        /**
         * Default LogicalTable factories that are defined in fili-core.
         *
         * @return  a Map of MetricMaker name to its factory
         */
        private Map<String, Factory<MetricMaker>> getDefaultMetricMakerFactories() {
            Map<String, Factory<MetricMaker>> metricMakerFactoryMap = new LinkedHashMap<>();

            addAliasesToFactory(
                    metricMakerFactoryMap,
                    new ArithmeticMakerFactory(),
                    ArithmeticMaker.class,
                    "arithmetic"
            );
            addAliasesToFactory(
                    metricMakerFactoryMap,
                    new LongSumMakerFactory(),
                    LongSumMaker.class,
                    "longSum"
            );
            addAliasesToFactory(
                    metricMakerFactoryMap,
                    new LongMaxMakerFactory(),
                    LongMaxMaker.class,
                    "longMax"
            );
            addAliasesToFactory(
                    metricMakerFactoryMap,
                    new LongMinMakerFactory(),
                    LongMinMaker.class,
                    "longMin"
            );
            addAliasesToFactory(
                    metricMakerFactoryMap,
                    new DoubleSumMakerFactory(),
                    DoubleSumMaker.class,
                    "doubleSum"
            );
            addAliasesToFactory(
                    metricMakerFactoryMap,
                    new DoubleMaxMakerFactory(),
                    DoubleMaxMaker.class,
                    "doubleMax"
            );
            addAliasesToFactory(
                    metricMakerFactoryMap,
                    new DoubleMinMakerFactory(),
                    DoubleMinMaker.class,
                    "doubleMin"
            );
            addAliasesToFactory(
                    metricMakerFactoryMap,
                    new AggregationAverageMakerFactory(),
                    AggregationAverageMaker.class,
                    "avg",
                    "average",
                    "aggregationAverage"
            );
            addAliasesToFactory(
                    metricMakerFactoryMap,
                    new CountMakerFactory(),
                    CountMaker.class,
                    "count"
            );

            return metricMakerFactoryMap;
        }

        /**
         * Registers type grouped dimension factories with the Industrial Park Builder.
         * <p>
         * There should be one factory per construction pattern for a concept in the system.
         *
         * @param conceptType The configuration concept being configured (e.g. Dimension, Metric..)
         * @param factories  A mapping from names of factories to a factory that builds instances of that type
         * @param <T> The configuration entity produced by this set of collection of factories.
         *
         * @return the builder object
         */
        public <T> Builder withFactories(ConceptType<T> conceptType, Map<String, Factory<T>> factories) {
            conceptFactoryMap.put(conceptType, factories);
            return this;
        }

        /**
         * Registers a collection of named factories for a given concept.
         * <p>
         * There should be one factory per construction pattern for a concept in the system.
         *
         * @param conceptType The configuration concept being configured (e.g. Dimension, Metric..)
         * @param factories  A mapping from names of factories to a factory that builds instances of that type
         * @param <T> The configuration entity produced by this set of collection of factories.
         *
         * @return the builder object
         */
        @SuppressWarnings("unchecked")
        public <T> Builder addFactories(ConceptType<T> conceptType, Map<String, Factory<T>> factories) {
            Map<String, Factory<T>> factory = (Map<String, Factory<T>>) conceptFactoryMap.computeIfAbsent(
                    conceptType,
                    (ignore) -> new LinkedHashMap<>()
            );
            factory.putAll(factories);
            return this;
        }

        /**
         * Registers a named factory with the Industrial Park Builder.
         * <p>
         * There should be one factory per construction pattern for a concept in the system.
         *
         * @param conceptType  The configuration type for the entity being produced
         * @param name  The identifier used in the configuration to identify the type of
         * dimension built by this factory
         * @param factory  A factory that builds Dimensions of the type named by {@code name}
         * @param <T>  The configuration entity produced by this set of collection of factories
         *
         * @return the builder object
         */
        @SuppressWarnings("unchecked")
        public <T> Builder addFactory(ConceptType<T> conceptType, String name, Factory<T> factory) {
            Map<String, Factory<T>> conceptFactory = (Map<String, Factory<T>>) conceptFactoryMap.get(conceptType);
            conceptFactory.put(name, factory);
            return this;
        }

        public GranularityDictionary getGranularityDictionary() {
            return granularityDictionary;
        }

        /**
         * Register one named Granularity into the GranularityDictionary.
         *
         * @param name  the identifier of the new granularity as a String
         * @param granularity  the new added granularity
         *
         * @return the builder object
         */
        public Builder addGranularity(String name, Granularity granularity) {
            this.granularityDictionary.put(name, granularity);
            return this;
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
        @SuppressWarnings("unchecked")
        public LuthierIndustrialPark build() {
            return new LuthierIndustrialPark(resourceDictionaries, granularityDictionary, conceptFactoryMap);
       }
    }
}
