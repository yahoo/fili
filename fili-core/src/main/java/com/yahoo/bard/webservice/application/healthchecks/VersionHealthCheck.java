// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.application.healthchecks;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigException;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import com.codahale.metrics.health.HealthCheck;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Displays version in /status.html
 */
public class VersionHealthCheck extends HealthCheck {

    private static final Logger LOG = LoggerFactory.getLogger(VersionHealthCheck.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    public static final String VERSION_KEY = SystemConfigProvider.getInstance().getPackageVariableName("version");

    private String version;

    public VersionHealthCheck() {
        this(VERSION_KEY);
    }

    public VersionHealthCheck(String key) {
        try {
            version = SYSTEM_CONFIG.getStringProperty(key);
        } catch (SystemConfigException ignored) {
            LOG.warn("{} not found in configuration", key);
            version = null;
        }
    }

    @Override
    protected Result check() throws Exception {
        return version == null ? Result.unhealthy(VERSION_KEY + " not set") : Result.healthy(version);
    }
}
