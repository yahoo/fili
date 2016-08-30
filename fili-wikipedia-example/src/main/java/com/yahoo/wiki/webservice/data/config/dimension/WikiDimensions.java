// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.dimension;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.MapStoreManager;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager;
import com.yahoo.bard.webservice.util.StreamUtils;
import com.yahoo.bard.webservice.util.Utils;
import com.yahoo.wiki.webservice.data.config.names.WikiApiDimensionName;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Hold all the dimension configurations for the sample Bard instance.
 */
public class WikiDimensions {

    private final SystemConfig systemConfig = SystemConfigProvider.getInstance();

    private final String defaultDimensionBackendKey = systemConfig.getPackageVariableName("dimension_backend");

    private final Set<DimensionConfig> dimensionConfigs;
    private final LinkedHashMap<String, DimensionConfig> wikiApiDimensionNameToConfig;

    /**
     * Construct the dimension configurations.
     */
    public WikiDimensions() {

        this.dimensionConfigs = Collections.unmodifiableSet(
                Arrays.stream(WikiApiDimensionName.values())
                        .map(
                                dimensionName -> new WikiDimensionConfig(
                                        dimensionName,
                                        dimensionName.asName(),
                                        getDefaultKeyValueStore(dimensionName),
                                        getDefaultSearchProvider(dimensionName),
                                        getDefaultFields()
                                )
                        )
                        .collect(Collectors.toSet())
        );

        wikiApiDimensionNameToConfig = dimensionConfigs.stream().collect(
                StreamUtils.toLinkedMap(DimensionConfig::getApiName, Function.identity())
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
     * Get dimension configurations provided the dimension api name.
     *
     * @param dimensionNames  Names for dimensions by api names
     *
     * @return set of dimension configurations
     */
    public LinkedHashSet<DimensionConfig> getDimensionConfigurationsByApiName(WikiApiDimensionName... dimensionNames) {
        return Arrays.stream(dimensionNames)
                .map(WikiApiDimensionName::asName)
                .map(wikiApiDimensionNameToConfig::get)
                .collect(Collectors.toCollection(LinkedHashSet<DimensionConfig>::new));
    }

    /**
     * Lazily provide a KeyValueStore for this store name.
     *
     * @param storeName  the name for the key value store
     *
     * @return A KeyValueStore instance
     */
    private KeyValueStore getDefaultKeyValueStore(WikiApiDimensionName storeName) {
        return MapStoreManager.getInstance(storeName.asName());
    }

    /**
     * Lazily create a Scanning Search Provider for this provider name.
     *
     * @param providerName  The name of the dimension's indexes
     *
     * @return  A Scanning Search Provider for the provider name.
     */
    private SearchProvider getDefaultSearchProvider(WikiApiDimensionName providerName) {
        return ScanSearchProviderManager.getInstance(providerName.asName());
    }

    private LinkedHashSet<DimensionField> getDefaultFields() {
        return Utils.<DimensionField>asLinkedHashSet(
                WikiDimensionField.ID,
                WikiDimensionField.DESC
        );
    }
}
