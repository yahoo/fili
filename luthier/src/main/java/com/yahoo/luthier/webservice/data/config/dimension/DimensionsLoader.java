// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.dimension;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.bard.webservice.data.config.dimension.DefaultKeyValueStoreDimensionConfig;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.MapStoreManager;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager;
import com.yahoo.bard.webservice.util.StreamUtils;
import com.yahoo.luthier.webservice.data.config.ExternalConfigLoader;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Hold all the dimension configurations for the sample Bard instance.
 */
public class DimensionsLoader {

    private final Set<DimensionConfig> dimensionConfigs;

    /**
     * Constructor using the default external configuration loader
     * and default external configuration file path.
     */
    public DimensionsLoader() {
        this(new ExternalConfigLoader(new ObjectMapper()),
                "DimensionConfigTemplateSample.json");
    }

    /**
     * Constructor using the default external configuration file path.
     *
     * @param dimensionConfigFilePath The external file's url containing the external config information
     */
    public DimensionsLoader(String dimensionConfigFilePath) {
        this(new ExternalConfigLoader(new ObjectMapper()),
                dimensionConfigFilePath);
    }

    /**
     * Construct the dimension configurations.
     *
     * @param dimensionConfigLoader   The external configuration loader for loading dimensions
     * @param dimensionConfigFilePath The external file's url containing the external config information
     */
    public DimensionsLoader(ExternalConfigLoader dimensionConfigLoader,
                            String dimensionConfigFilePath) {

        DimensionConfigTemplate dimensionConfig =
                dimensionConfigLoader.parseExternalFile(
                        dimensionConfigFilePath,
                        DimensionConfigTemplate.class);

        this.dimensionConfigs = Collections.unmodifiableSet(
                dimensionConfig.getDimensions().stream()
                        .map(
                                dimensionName -> new DefaultKeyValueStoreDimensionConfig(
                                        dimensionName,
                                        dimensionName.asName(),
                                        dimensionName.getDescription(),
                                        dimensionName.getLongName(),
                                        dimensionName.getCategory(),
                                        dimensionName.getFields(dimensionConfig.getFieldSets()),
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
     * Get dimension configurations dictionary.
     *
     * @return a map from dimension name to dimension configurations
     */
    public Map<String, DimensionConfig> getDimensionConfigurations() {
        Map<String, DimensionConfig> dimensionConfigMap = new HashMap<>();
        for (DimensionConfig dimensionConfig : dimensionConfigs) {
            dimensionConfigMap.put(dimensionConfig.getApiName(), dimensionConfig);
        }
        return dimensionConfigMap;
    }

    /**
     * Lazily provide a KeyValueStore for this store name.
     *
     * @param storeName the name for the key value store
     * @return A KeyValueStore instance
     */
    private KeyValueStore getDefaultKeyValueStore(DimensionTemplate storeName) {
        return MapStoreManager.getInstance(storeName.asName());
    }

    /**
     * Lazily create a Scanning Search Provider for this provider name.
     *
     * @param providerName The name of the dimension's indexes
     * @return A Scanning Search Provider for the provider name.
     */
    private SearchProvider getDefaultSearchProvider(DimensionTemplate providerName) {
        return ScanSearchProviderManager.getInstance(providerName.asName());
    }
}
