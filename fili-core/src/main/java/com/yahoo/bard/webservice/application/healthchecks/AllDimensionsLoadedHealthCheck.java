// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application.healthchecks;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.util.StreamUtils;

import com.codahale.metrics.health.HealthCheck;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * HealthCheck that verifies all dimensions have had a lastUpdated time set.
 */
public class AllDimensionsLoadedHealthCheck extends HealthCheck {

    /**
     * A filtering string that indicates that a dimension has not been loaded.
     */
    private static final String NEVER = "never";

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
        // Gather information about what has been loaded and what hasn't
        Map<String, String> dimensionLastUpdated = dimensionDictionary.findAll().stream()
                .collect(
                        StreamUtils.toLinkedMap(
                                Dimension::getApiName,
                                dim -> dim.getLastUpdated() == null ? NEVER : dim.getLastUpdated().toString()
                        )
                );

        // Signal health
        if (dimensionLastUpdated.containsValue(NEVER)) {
            return Result.unhealthy(
                    String.format(
                            "These dimensions have not been loaded: %s",
                            dimensionLastUpdated.entrySet().stream()
                                    .filter(entry -> NEVER.equals(entry.getValue()))
                                    .map(Map.Entry::getKey)
                                    .collect(Collectors.toSet())
                    )
            );
        } else {
            return Result.healthy(String.format("Dimensions have all been loaded: %s", dimensionLastUpdated));
        }
    }

    public DimensionDictionary getDimensionDictionary() {
        return dimensionDictionary;
    }
}
