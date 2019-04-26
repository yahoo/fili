// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class to provide a system configuration instance.
 */
public class SystemConfigProvider {

    private static final Logger LOG = LoggerFactory.getLogger(SystemConfigProvider.class);

    private static final String SYSTEM_CONFIG_IMPL_KEY = "fili__system_config_impl";

    private static final String DEFAULT_SYSTEM_CONFIG_IMPL = LayeredFileSystemConfig.class.getCanonicalName();

    /**
     *  The instance of the System Config available in this system.
     */
    private static final SystemConfig SYSTEM_CONFIG = getInstance();

    /**
     * Get an instance of SystemConfiguration.
     *
     * @return an instance of SystemConfiguration
     */
    public static synchronized SystemConfig getInstance() {
        if (SYSTEM_CONFIG == null) {
            String systemConfigImplementation = System.getenv(SYSTEM_CONFIG_IMPL_KEY);
            try {
                if (systemConfigImplementation == null) {
                    systemConfigImplementation = System.getProperty(SYSTEM_CONFIG_IMPL_KEY, DEFAULT_SYSTEM_CONFIG_IMPL);
                }
                return Class.forName(systemConfigImplementation).asSubclass(SystemConfig.class).newInstance();
            } catch (Exception e) {
                LOG.error("Exception while loading System configuration: {}", e.getMessage(), e);
                throw new IllegalStateException(e);
            }
        }
        return SYSTEM_CONFIG;
    }
}
