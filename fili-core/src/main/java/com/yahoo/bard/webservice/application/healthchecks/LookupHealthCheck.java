// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application.healthchecks;

import com.yahoo.bard.webservice.metadata.RegisteredLookupMetadataLoadTask;

import com.codahale.metrics.health.HealthCheck;

import java.util.Set;

import javax.inject.Singleton;

/**
 * Check load statuses of all Druid lookups.
 */
@Singleton
public class LookupHealthCheck extends HealthCheck {
    private final RegisteredLookupMetadataLoadTask registeredLookupMetadataLoadTask;

    /**
     * Constructor.
     *
     * @param registeredLookupMetadataLoadTask  A {@link RegisteredLookupMetadataLoadTask} that keeps load statuses of
     * all Druid lookups
     */
    public LookupHealthCheck(RegisteredLookupMetadataLoadTask registeredLookupMetadataLoadTask) {
        this.registeredLookupMetadataLoadTask = registeredLookupMetadataLoadTask;
    }

    @Override
    public Result check() {
        Set<String> unloadedLookups = registeredLookupMetadataLoadTask.getPendingLookups();
        return unloadedLookups.isEmpty()
                ? Result.healthy("All Druid lookups have been loaded.")
                : Result.unhealthy("Lookups %s are not loaded.", unloadedLookups);
    }
}
