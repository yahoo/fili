// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.wiki.webservice.data.config.auto.DataSourceConfiguration;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Hold all the dimension configurations for a generic druid configuration.
 */
public class GenericDimensionConfigs {
    private final Set<DimensionConfig> allDimensionConfigs;

    /**
     * Construct the dimension configurations.
     *
     * @param configLoader  Supplies DataSourceConfigurations to build the dimensions from.
     */
    public GenericDimensionConfigs(Supplier<List<? extends DataSourceConfiguration>> configLoader) {
        allDimensionConfigs = configLoader.get().stream()
                .map(DataSourceConfiguration::getDimensionConfigs)
                .flatMap(Set::stream)
                .collect(Collectors.collectingAndThen(
                        Collectors.toSet(),
                        Collections::unmodifiableSet
                ));
    }

    /**
     * Get all dimension configurations.
     *
     * @return set of dimension configurations
     */
    public Set<DimensionConfig> getAllDimensionConfigurations() {
        return allDimensionConfigs;
    }
}
