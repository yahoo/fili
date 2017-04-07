// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.dimension;

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
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Hold all the dimension configurations for a generic druid configuration.
 */
public class GenericDimensions {
    private final Set<DimensionConfig> dimensionConfigs;

    /**
     * Construct the dimension configurations.
     * @param configLoader  Supplies DataSourceConfigurations to build the dimensions from.
     */
    public GenericDimensions(Supplier<List<? extends DataSourceConfiguration>> configLoader) {
        dimensionConfigs = configLoader.get().stream()
                .flatMap(tableName -> tableName.getDimensions().stream())
                .map(dimensionName -> new GenericDimensionConfig(
                        dimensionName,
                        dimensionName,
                        getDefaultKeyValueStore(dimensionName),
                        getDefaultSearchProvider(dimensionName),
                        getDefaultFields()
                ))
                .collect(
                        Collectors.collectingAndThen(
                                Collectors.toSet(),
                                Collections::unmodifiableSet
                        )
                );
    }

    /**
     * Get all dimension configurations.
     *
     * @return set of dimension configurations
     */
    public Set<DimensionConfig> getAllDimensionConfigurations() {
        return dimensionConfigs;
    }

    /**
     * Lazily provide a KeyValueStore for this store name.
     *
     * @param storeName  the name for the key value store
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
                GenericDimensionField.ID);
    }
}
