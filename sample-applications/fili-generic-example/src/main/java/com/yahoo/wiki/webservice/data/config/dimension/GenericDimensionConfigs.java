// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.config.dimension.DefaultDimensionField;
import com.yahoo.bard.webservice.data.config.dimension.DefaultKeyValueStoreDimensionConfig;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.MapStoreManager;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager;
import com.yahoo.bard.webservice.util.Utils;
import com.yahoo.wiki.webservice.data.config.auto.DataSourceConfiguration;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Hold all the dimension configurations for a generic druid configuration.
 */
public class GenericDimensionConfigs {
    private final Map<String, Set<DimensionConfig>> dataSourceToDimensionConfigs;

    /**
     * Construct the dimension configurations.
     *
     * @param configLoader  Supplies DataSourceConfigurations to build the dimensions from.
     */
    public GenericDimensionConfigs(Supplier<List<? extends DataSourceConfiguration>> configLoader) {
        dataSourceToDimensionConfigs = new HashMap<>();
        configLoader.get()
                .forEach(dataSourceConfiguration -> {
                    Set<DimensionConfig> tableDimensionConfigs = dataSourceConfiguration.getDimensions().stream()
                            .map(dimensionName -> new DefaultKeyValueStoreDimensionConfig(
                                            () -> dimensionName,
                                            dimensionName,
                                            "",
                                            dimensionName,
                                            "General",
                                            getDefaultFields(),
                                            getDefaultKeyValueStore(dimensionName),
                                            getDefaultSearchProvider(dimensionName)
                                    )
                            ).collect(
                                    Collectors.collectingAndThen(
                                            Collectors.toSet(),
                                            Collections::unmodifiableSet
                                    ));

                    dataSourceToDimensionConfigs.put(dataSourceConfiguration.getName(), tableDimensionConfigs);
                });
    }

    /**
     * Get all dimension configurations of all data sources.
     *
     * @return set of dimension configurations
     */
    public Set<DimensionConfig> getAllDimensionConfigurations() {
        return dataSourceToDimensionConfigs.values().stream()
                .flatMap(Set::stream)
                .distinct()
                .collect(Collectors.toSet());
    }

    /**
     * Get all the dimension configurations associated with this datasource.
     *
     * @param dataSourceConfiguration  The datasource configuration's dimensions to load
     *
     * @return the dimension configurations for this datasource
     *
     * @deprecated only the name(a String) of DataSourceConfiguration can resolve its set of DimensionConfigs. There is
     * no need to pass a heavier DataSourceConfiguration object. Use {@link #getDimensionConfigs(String)} instead.
     */
    @Deprecated
    public Set<DimensionConfig> getDimensionConfigs(DataSourceConfiguration dataSourceConfiguration) {
        return dataSourceToDimensionConfigs.getOrDefault(dataSourceConfiguration.getName(), Collections.emptySet());
    }

    /**
     * Returns all dimension configurations of a particular data source.
     *
     * @param dataSourceName  Name of the data source
     *
     * @return all dimension configurations of the particular data source
     */
    public Set<DimensionConfig> getDimensionConfigs(String dataSourceName) {
        return dataSourceToDimensionConfigs.getOrDefault(dataSourceName, Collections.emptySet());
    }

    /**
     * Lazily provide a KeyValueStore for this store name.
     *
     * @param storeName  The name for the key value store
     *
     * @return A KeyValueStore instance
     */
    private KeyValueStore getDefaultKeyValueStore(String storeName) {
        return MapStoreManager.getInstance(storeName);
    }

    /**
     * Lazily create a Scanning Search Provider for this provider name.
     *
     * @param providerName  The name of the dimension's indexes
     *
     * @return A Scanning Search Provider for the provider name.
     */
    private SearchProvider getDefaultSearchProvider(String providerName) {
        return ScanSearchProviderManager.getInstance(providerName);
    }

    private LinkedHashSet<DimensionField> getDefaultFields() {
        return Utils.asLinkedHashSet(
                DefaultDimensionField.ID);
    }
}
