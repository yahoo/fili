// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Known dimension backends.
 */
public enum DimensionBackend {

    REDIS,
    MEMORY;

    private static final Logger LOG = LoggerFactory.getLogger(DimensionBackend.class);

    protected static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private final static String DIMENSION_BACKEND_KEY = SYSTEM_CONFIG.getPackageVariableName("dimension_backend");

    /**
     * Get the DimensionBackend for tests.
     *
     * @return the configured dimension backend
     */
    public static DimensionBackend getBackend() {
        // Read every time, since some tests set dimension_backend

        String dimensionBackend = SYSTEM_CONFIG.getStringProperty(DIMENSION_BACKEND_KEY, "memory");
        if ("redis".equalsIgnoreCase(dimensionBackend)) {
            return REDIS;
        }
        return MEMORY;
    }
}
