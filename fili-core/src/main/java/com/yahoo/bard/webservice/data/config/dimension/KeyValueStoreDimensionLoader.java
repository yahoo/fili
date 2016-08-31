// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import com.yahoo.bard.webservice.application.HealthCheckRegistryFactory;
import com.yahoo.bard.webservice.application.healthchecks.KeyValueStoreHealthCheck;
import com.yahoo.bard.webservice.application.healthchecks.SearchProviderHealthCheck;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension;

import com.codahale.metrics.health.HealthCheckRegistry;

/**
 * Load the dimensions along with its keyValueStore and searchProvider.
 *
 * @deprecated in favor of TypeAwareDimensionLoader which loads corresponding dimension based on config type
 */
@Deprecated
public class KeyValueStoreDimensionLoader implements DimensionLoader {

    private Iterable<DimensionConfig> configSource;

    /**
     * Constructor.
     *
     * @param configSource  DimensionConfigs to load as KeyValueStoreDimensions.
     */
    public KeyValueStoreDimensionLoader(Iterable<DimensionConfig> configSource) {
        this.configSource = configSource;
    }

    @Override
    public void loadDimensionDictionary(DimensionDictionary dimensions) {
        HealthCheckRegistry healthCheckRegistry = HealthCheckRegistryFactory.getRegistry();
        for (DimensionConfig dimension : configSource) {
            dimensions.add(new KeyValueStoreDimension(dimension));

            //Register KeyValueStore Health Check
            healthCheckRegistry.register(
                dimension.getApiName() + " keyValueStore check",
                new KeyValueStoreHealthCheck(dimension.getKeyValueStore())
            );

            //Register SearchProvider Health Check
            healthCheckRegistry.register(
                dimension.getApiName() + " searchProvider check",
                new SearchProviderHealthCheck(dimension.getSearchProvider())
            );
        }
    }
}
