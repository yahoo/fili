// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers.workflow;

import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Caching config analyzer.
 */
public final class CacheMode {
    private static final Logger LOG = LoggerFactory.getLogger(CacheMode.class);

    private static final boolean CACHE_V1 = BardFeatureFlag.DRUID_CACHE.isOn();
    private static final boolean CACHE_V2 = BardFeatureFlag.DRUID_CACHE_V2.isOn();
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final String QUERY_RESPONSE_CACHING_STRATEGY = SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("query_response_caching_strategy"),
            "None"
    );

    /**
     * Returns the caching mode in String.
     *
     * @return the caching mode in String
     */
    public static Optional<Mode> getCacheMode() {
        // no etag cache, either TTL or local signature
        if (QUERY_RESPONSE_CACHING_STRATEGY.equals(Mode.NONE.getMode())) {
            if (CACHE_V1 && !CACHE_V2) {
                return Optional.of(Mode.TTL_ONLY);
            } else {
                return Optional.of(Mode.LOCAL_SIGNATURE);
            }
        } else if (!CACHE_V1 && !CACHE_V2) { // TTL and local signature is not set
            return QUERY_RESPONSE_CACHING_STRATEGY.equals(Mode.NONE.getMode())
                    ? Optional.empty()
                    : Optional.of(Mode.ETAG);
        }

        String message = "Etag caching cannot coexist with TTL or local signature caching";
        LOG.error(message);
        throw new RuntimeException(message);
    }

    /**
     * The representation of caching mode.
     */
    public enum Mode {
        /**
         * No caching is configured.
         */
        NONE("None"),
        /**
         * Use only TTL caching.
         */
        TTL_ONLY("TtlOnly"),
        /**
         * Use only local signature caching.
         */
        LOCAL_SIGNATURE("LocalSignature"),
        /**
         * Use only etag caching.
         */
        ETAG("ETag");

        private final String mode;

        /**
         * Constructor.
         *
         * @param mode the mode in String representation
         */
        Mode(final String mode) {
            this.mode = mode;
        }

        /**
         * Returns the caching mode in String representation.
         *
         * @return the caching mode in String representation
         */
        public String getMode() {
            return mode;
        }
    }
}
