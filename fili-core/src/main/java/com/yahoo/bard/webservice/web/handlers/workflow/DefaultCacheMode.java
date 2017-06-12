// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers.workflow;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import java.util.Optional;

/**
 * DefaultCacheMode are a set of concrete CacheMode implementations which support TTL cache, local signature cache, and
 * eTag cache.
 */
public enum DefaultCacheMode implements CacheMode {

    /**
     * No caching.
     */
    NONE("None"),
    /**
     * Use only TTL caching.
     */
    TTL_ONLY("TtlOnly"),
    /**
     * Use only local signature caching.
     */
    LOCAL_SIGNATURE("LocalSignatureOnly"),
    /**
     * Use only etag caching.
     */
    ETAG("ETagOnly");

    private final SystemConfig systemConfig = SystemConfigProvider.getInstance();
    private final String queryResponseCachingStragety = systemConfig.getStringProperty(
            systemConfig.getPackageVariableName("query_response_caching_strategy"),
            "None"
    );

    private final String mode;
    private final Boolean set;

    /**
     * Constructor.
     *
     * @param mode  The mode in String representation
     */
    DefaultCacheMode(final String mode) {
        this.mode = mode;
        this.set = mode.equalsIgnoreCase(queryResponseCachingStragety);
    }

    @Override
    public Optional<Boolean> isSet() {
        return "None".equalsIgnoreCase(mode) ? Optional.empty() : Optional.of(set);
    }
}
