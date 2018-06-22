// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.config.dimension.DefaultKeyValueStoreDimensionConfig;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.MapStoreManager;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager;
import com.yahoo.bard.webservice.util.StreamUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.wiki.webservice.data.config.ExternalConfigLoader;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Hold all the dimension configurations for the sample Bard instance.
 */
public class WikiDimensionsLoader {

    private final Set<DimensionConfig> dimensionConfigs;

    /**
     * Constructor.
     */
    public WikiDimensionsLoader() {
        this(new ObjectMapper());
    }

    /**
     * Construct the dimension configurations.
     *
     * @param objectMapper objectMapper for external file parse
     */
    public WikiDimensionsLoader(ObjectMapper objectMapper) {

        ExternalConfigLoader dimensionConfigLoader = new ExternalConfigLoader(objectMapper);
        WikiDimensionConfigTemplate wikiDimensionConfig =
                dimensionConfigLoader.parseExternalFile(
                        "DimensionConfigTemplateSample.json",
                        WikiDimensionConfigTemplate.class);

        this.dimensionConfigs = Collections.unmodifiableSet(
                wikiDimensionConfig.getDimensions().stream()
                        .map(
                                dimensionName -> new DefaultKeyValueStoreDimensionConfig(
                                        dimensionName,
                                        dimensionName.asName(),
                                        dimensionName.getDescription(),
                                        dimensionName.getLongName(),
                                        dimensionName.getCategory(),
                                        dimensionName.getFields(wikiDimensionConfig.getFieldSets()),
                                        getDefaultKeyValueStore(dimensionName),
                                        getDefaultSearchProvider(dimensionName)
                                )
                        )
                        .collect(Collectors.toSet())
        );

        dimensionConfigs.stream().collect(
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
     * @return set of dimension configurations
     */
    public LinkedHashSet<DimensionConfig> getDimensionConfigurationsByConfigInfo() {
        return new LinkedHashSet<>(dimensionConfigs);
    }

    /**
     * Lazily provide a KeyValueStore for this store name.
     *
     * @param storeName the name for the key value store
     * @return A KeyValueStore instance
     */
    private KeyValueStore getDefaultKeyValueStore(WikiDimensionTemplate storeName) {
        return MapStoreManager.getInstance(storeName.asName());
    }

    /**
     * Lazily create a Scanning Search Provider for this provider name.
     *
     * @param providerName The name of the dimension's indexes
     * @return A Scanning Search Provider for the provider name.
     */
    private SearchProvider getDefaultSearchProvider(WikiDimensionTemplate providerName) {
        return ScanSearchProviderManager.getInstance(providerName.asName());
    }
}
