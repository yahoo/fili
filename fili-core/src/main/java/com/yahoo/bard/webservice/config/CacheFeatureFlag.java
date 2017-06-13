// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config;

/**
 * Binds a caching strategy to a system configuration name.
 */
public enum CacheFeatureFlag implements FeatureFlag {
    /**
     * No cache.
     */
    NONE("query_response_caching_strategy", "NoCache"),
    /**
     * Use only TTL cache.
     */
    TTL("query_response_caching_strategy", "Ttl"),
    /**
     * Use only local signature cache.
     */
    LOCAL_SIGNATURE("query_response_caching_strategy", "LocalSignature"),
    /**
     * Use only etag cache.
     */
    ETAG("query_response_caching_strategy", "ETag");

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final String QUERY_RESPONSE_CACHING_STRAGEGY = SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("query_response_caching_strategy"),
            "None"
    );

    private final String propertyName;
    private final String value;

    /**
     * Constructor.
     *
     * @param propertyName  Name of the SystemConfig property to use for the feature flag.
     * @param value  Value configured for the propertyName
     */
    CacheFeatureFlag(final String propertyName, final String value) {
        this.propertyName = propertyName;
        this.value = value;
    }

    @Override
    public String getName() {
        return propertyName;
    }

    @Override
    public boolean isOn() {
        // TODO: Remove this if conditional after cache V1 & V2 are removed
        if (BardFeatureFlag.DRUID_CACHE.isSet() || BardFeatureFlag.DRUID_CACHE_V2.isSet()) {
            return (this.value.equals("Ttl") && !BardFeatureFlag.DRUID_CACHE_V2.isOn())
                    || (this.value.equals("LocalSignature") && BardFeatureFlag.DRUID_CACHE_V2.isOn());
        }
        return QUERY_RESPONSE_CACHING_STRAGEGY.equalsIgnoreCase(value);
    }

    @Override
    public void setOn(Boolean newValue) {
        return;
    }
}
