// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import com.yahoo.bard.webservice.application.HealthCheckRegistryFactory;
import com.yahoo.bard.webservice.application.healthchecks.KeyValueStoreHealthCheck;
import com.yahoo.bard.webservice.application.healthchecks.SearchProviderHealthCheck;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension;
import com.yahoo.bard.webservice.data.dimension.impl.LookupDimension;

import com.codahale.metrics.health.HealthCheckRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load dimensions based on the type of the dimension.
 */
public class TypeAwareDimensionLoader implements DimensionLoader {

    private static final Logger LOG = LoggerFactory.getLogger(TypeAwareDimensionLoader.class);
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
        for (DimensionConfig dimension : configSource) {
            if (dimension.getType() == KeyValueStoreDimension.class) {
                dimensions.add(new KeyValueStoreDimension(dimension));
                registerHealthChecks(dimension);
            } else if (dimension.getType() == LookupDimension.class) {
                dimensions.add(new LookupDimension((LookupDimensionConfig) dimension));
                registerHealthChecks(dimension);
            } else {
                LOG.warn(
                    String.format(
                        "The dimension type for the dimension %s is not defined",
                        dimension.getApiName())
                );
            }
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
