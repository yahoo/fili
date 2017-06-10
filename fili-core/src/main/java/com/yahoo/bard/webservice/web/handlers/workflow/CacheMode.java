// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers.workflow;

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

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final String CACHE_V1 = SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("druid_cache_enabled"),
            ""
    );
    private static final String CACHE_V2 = SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("druid_cache_v2_enabled"),
            ""
    );
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
        // if either cache v1 or v2 is set, use old caching logic
        if (!CACHE_V1.isEmpty() || !CACHE_V2.isEmpty()) {
            LOG.warn(
                    "Cache V1(TTL) and V2(Local Signature) are deprecated, consider etag cache as new caching strategy."
            );
            if (CACHE_V1.isEmpty()) {
                return Optional.of(Mode.LOCAL_SIGNATURE);
            } else if (CACHE_V2.isEmpty()) {
                return Optional.of(Mode.TTL_ONLY);
            } else if ("true".equalsIgnoreCase(CACHE_V1) && "true".equalsIgnoreCase(CACHE_V2)) {
                return Optional.of(Mode.LOCAL_SIGNATURE);
            } else if ("true".equalsIgnoreCase(CACHE_V1) &&
                    "false".equalsIgnoreCase(CACHE_V2)) {
                return Optional.of(Mode.TTL_ONLY);
            } else {
                return Optional.of(Mode.NONE);
            }
        } else if ("TtlOnly".equalsIgnoreCase(QUERY_RESPONSE_CACHING_STRATEGY)) {
            return Optional.of(Mode.TTL_ONLY);
        } else if ("LocalSignature".equalsIgnoreCase(QUERY_RESPONSE_CACHING_STRATEGY)) {
            return Optional.of(Mode.LOCAL_SIGNATURE);
        } else if ("ETag".equalsIgnoreCase(QUERY_RESPONSE_CACHING_STRATEGY)) {
            return Optional.of(Mode.ETAG);
        } else {
            return Optional.empty();
        }
    }

    /**
     * The representation of caching mode.
     */
    public enum Mode {
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
