// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application.healthchecks;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;

import com.codahale.metrics.health.HealthCheck;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * HealthCheck that verifies all dimensions have had a lastUpdated time set.
 */
public class AllDimensionsLoadedHealthCheck extends HealthCheck {

    private final DimensionDictionary dimensionDictionary;

    /**
     * Construct a new AllDimensionsLoadedHealthCheck.
     *
     * @param dimensionDictionary  DimensionDictionary to check the dimensions of.
     */
    public AllDimensionsLoadedHealthCheck(@NotNull DimensionDictionary dimensionDictionary) {
        this.dimensionDictionary = Objects.requireNonNull(dimensionDictionary, "A DimensionDictionary is required.");
    }

    @Override
    protected Result check() throws Exception {
        // Gather information about what has not been loaded
        Set<String> notLoaded = dimensionDictionary.findAll().stream()
                .filter(dim -> dim.getLastUpdated() == null)
                .map(Dimension::getApiName)
                .collect(Collectors.toSet());

        // Signal health
        if (notLoaded.isEmpty()) {
            return Result.healthy("Dimensions have all been loaded");
        } else {
            return Result.unhealthy(String.format("These dimensions have not been loaded: %s", notLoaded));
        }
    }

    public DimensionDictionary getDimensionDictionary() {
        return dimensionDictionary;
    }
}
