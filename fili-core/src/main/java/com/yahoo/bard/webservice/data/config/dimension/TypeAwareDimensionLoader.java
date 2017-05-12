// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DIMENSION_TYPE_INVALID;

import com.yahoo.bard.webservice.application.HealthCheckRegistryFactory;
import com.yahoo.bard.webservice.application.healthchecks.KeyValueStoreHealthCheck;
import com.yahoo.bard.webservice.application.healthchecks.SearchProviderHealthCheck;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension;
import com.yahoo.bard.webservice.data.dimension.impl.LookupDimension;
import com.yahoo.bard.webservice.data.dimension.impl.RegisteredLookupDimension;

import com.codahale.metrics.health.HealthCheckRegistry;

/**
 * Load dimensions based on the type of the dimension.
 */
public class TypeAwareDimensionLoader implements DimensionLoader {

    private final Iterable<DimensionConfig> configSource;
    private final HealthCheckRegistry healthCheckRegistry;

    /**
     * Constructor.
     *
     * @param configSource DimensionConfigs to load the dimensions.
     */
    public TypeAwareDimensionLoader(Iterable<DimensionConfig> configSource) {
        this.configSource = configSource;
        healthCheckRegistry = HealthCheckRegistryFactory.getRegistry();
    }

    @Override
    public void loadDimensionDictionary(DimensionDictionary dimensions) {
        for (DimensionConfig dimensionConfig : configSource) {
            if (dimensionConfig.getType().equals(KeyValueStoreDimension.class)) {
                dimensions.add(new KeyValueStoreDimension(dimensionConfig));
            } else if (dimensionConfig.getType().equals(LookupDimension.class)) {
                dimensions.add(new LookupDimension((LookupDimensionConfig) dimensionConfig));
            } else if (dimensionConfig.getType().equals(RegisteredLookupDimension.class)) {
                dimensions.add(new RegisteredLookupDimension((RegisteredLookupDimensionConfig) dimensionConfig));
            } else {
                throw new RuntimeException(
                        DIMENSION_TYPE_INVALID.format(
                                dimensionConfig.getType(),
                                dimensionConfig.getApiName()
                        )
                );
            }
            registerHealthChecks(dimensionConfig);
        }
    }

    /**
     * Registers Health Checks.
     *
     * @param dimension Dimension to be registered for health checks
     */
    private void registerHealthChecks(DimensionConfig dimension) {
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
