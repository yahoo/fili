// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application.healthchecks;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import com.codahale.metrics.health.HealthCheck;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Displays version in /status.html.
 */
public class VersionHealthCheck extends HealthCheck {

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private static final Logger LOG = LoggerFactory.getLogger(VersionHealthCheck.class);
    public static final String VERSION_KEY = SYSTEM_CONFIG.getPackageVariableName("version");
    public static final String GIT_SHA_KEY = SYSTEM_CONFIG.getPackageVariableName("git_sha");

    private final String usedVersionKey;
    private final String version;
    private final String gitSha;

    /**
     * Constructor using the default version key property name.
     */
    public VersionHealthCheck() {
        this(VERSION_KEY, GIT_SHA_KEY);
    }

    /**
     * Constructor.
     *
     * @param versionKey  The property name from which to get the version from the SystemConfig
     * @param gitShaKey  The property name from which to get the git sha from the SystemConfig
     */
    public VersionHealthCheck(String versionKey, String gitShaKey) {
        usedVersionKey = versionKey;
        version = SYSTEM_CONFIG.getStringProperty(versionKey, null);
        if (version == null) {
            LOG.warn("{} not found in configuration", versionKey);
        }

        gitSha = SYSTEM_CONFIG.getStringProperty(gitShaKey, null);
        if (gitSha == null) {
            LOG.warn("{} not found in configuration", gitShaKey);
        }
    }

    @Override
    protected Result check() throws Exception {
        return version == null ? Result.unhealthy(usedVersionKey + " not set") : Result.healthy(version + ":" + gitSha);
    }
}
