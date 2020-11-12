// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config;

import com.yahoo.bard.webservice.util.Utils;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;

import org.glassfish.hk2.api.IterableProvider;

import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.StreamSupport;

import javax.inject.Inject;

/**
 * Feature flags registry to keep mappings between feature flag names and enumerations.
 */
public class FeatureFlagRegistry {
    private static final Map<String, FeatureFlag> NAMES_TO_VALUES = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param featureFlags  Feature flags to register initially.
     */
    @Inject
    public FeatureFlagRegistry(IterableProvider<FeatureFlag> featureFlags) {
        StreamSupport.stream(featureFlags.spliterator(), false).forEach(this::add);
    }

    /**
     * Get a registered FeatureFlag by name.
     *
     * @param name  Name of the FeatureFlag to get
     *
     * @return The feature flag with the given name
     * @throws BadApiRequestException if no feature flag has been registered for that name
     */
    public FeatureFlag forName(String name) throws BadApiRequestException {
        FeatureFlag flag = NAMES_TO_VALUES.get(name.toUpperCase(Locale.ENGLISH));
        return flag != null ? flag : Utils.insteadThrowRuntime(
                new BadApiRequestException("Invalid feature flag: " + name)
        );
    }

    public Collection<FeatureFlag> getValues() {
        return NAMES_TO_VALUES.values();
    }

    /**
     * Register the feature flag.
     *
     * @param featureFlag  FeatureFlag to register.
     */
    public void add(FeatureFlag featureFlag) {
        NAMES_TO_VALUES.put(featureFlag.name() + "_ENABLED", featureFlag);
    }

    /**
     * Register all of the feature flags.
     *
     * @param featureFlags  FeatureFlags to register
     */
    public void addAll(Collection<FeatureFlag> featureFlags) {
        featureFlags.stream().forEach(this::add);
    }
}
