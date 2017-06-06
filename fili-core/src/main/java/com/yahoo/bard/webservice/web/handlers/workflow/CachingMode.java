// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers.workflow;

import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Caching config analyzer.
 */
public class CachingMode {
    private static final Logger LOG = LoggerFactory.getLogger(CachingMode.class);

    private static final boolean CACHE_V1 = BardFeatureFlag.DRUID_CACHE.isOn();
    private static final boolean CACHE_V2 = BardFeatureFlag.DRUID_CACHE_V2.isOn();
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final int DRUID_UNCOVERED_INTERVAL_LIMIT = SYSTEM_CONFIG.getIntProperty(
            SYSTEM_CONFIG.getPackageVariableName("druid_uncovered_interval_limit"),
            0
    );
    private static final String NO_CACHING = "No caching";
    private static final String TTL_CACHING = "TTL caching only";
    private static final String LOCAL_SIGNATURE_CACHING = "Local signature caching only";
    private static final String ETAG_CACHING = "Etag caching only";

    /**
     * Returns the caching mode in String.
     *
     * @return the caching mode in String
     */
    public static String getCachingMode() {
        if (!CACHE_V1 && !CACHE_V2 && DRUID_UNCOVERED_INTERVAL_LIMIT <= 0) {
            return NO_CACHING;
        } else if (CACHE_V1 && !CACHE_V2 && DRUID_UNCOVERED_INTERVAL_LIMIT <= 0) {
            return TTL_CACHING;
        } else if (!CACHE_V1 && CACHE_V2 && DRUID_UNCOVERED_INTERVAL_LIMIT <= 0) {
            return LOCAL_SIGNATURE_CACHING;
        } else if (!CACHE_V1 && !CACHE_V2 && DRUID_UNCOVERED_INTERVAL_LIMIT > 0) {
            return ETAG_CACHING;
        } else if ((CACHE_V1 || CACHE_V2) && DRUID_UNCOVERED_INTERVAL_LIMIT > 0) {
            String message = "Etag caching cannot coexist with TTL or local signature caching";
            LOG.error(message);
            throw new RuntimeException(message);
        } else {
            return ETAG_CACHING;
        }
    }

    /**
     * The representation of caching mode.
     */
    public enum Mode {
        /**
         * No caching is configured.
         */
        NONE(NO_CACHING),
        /**
         * Use only TTL caching.
         */
        TTL_ONLY(TTL_CACHING),

        /**
         * Use only local signature caching.
         */
        LOCAL_SIGNATURE(LOCAL_SIGNATURE_CACHING),
        /**
         * Use only etag caching.
         */
        ETAG(ETAG_CACHING);

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
