// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Binds a caching strategy to a system configuration name.
 */
public enum CacheFeatureFlag implements FeatureFlag {
    /**
     * No cache.
     */
    NONE("NoCache"),
    /**
     * Use only TTL cache.
     */
    TTL("Ttl"),
    /**
     * Use only local signature cache.
     */
    LOCAL_SIGNATURE("LocalSignature"),
    /**
     * Use only etag cache.
     */
    ETAG("ETag");

    private static final Logger LOG = LoggerFactory.getLogger(CacheFeatureFlag.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private final String value;

    /**
     * Constructor.
     *
     * @param value  Value configured for the propertyName
     */
    CacheFeatureFlag(final String value) {
        this.value = value;
    }

    @Override
    public String getName() {
        return value;
    }

    @Override
    public boolean isOn() {
        // TODO: Remove this if conditional after cache V1 & V2 configuration flags are removed
        if (BardFeatureFlag.DRUID_CACHE.isSet() || BardFeatureFlag.DRUID_CACHE_V2.isSet()) {
            // no cache
            if (this.value.equals("NoCache")) {
                return ! BardFeatureFlag.DRUID_CACHE.isOn();
            }

            return (this.value.equals("Ttl")
                    && !BardFeatureFlag.DRUID_CACHE_V2.isOn()
                    && BardFeatureFlag.DRUID_CACHE.isOn()
            )
                    || (this.value.equals("LocalSignature")
                    && BardFeatureFlag.DRUID_CACHE.isOn()
                    && BardFeatureFlag.DRUID_CACHE_V2.isOn());
        }
        return value.equalsIgnoreCase(
                SYSTEM_CONFIG.getStringProperty(
                        SYSTEM_CONFIG.getPackageVariableName("query_response_caching_strategy"),
                        "NoCache"
                )
        );
    }

    @Override
    public void setOn(Boolean newValue) {
        LOG.warn("setOn(Boolean) method does not apply in CacheFeatureFlag");
    }
}
