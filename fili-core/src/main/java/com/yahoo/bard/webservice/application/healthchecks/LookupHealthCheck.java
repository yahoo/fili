// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application.healthchecks;

import com.yahoo.bard.webservice.metadata.LookupMetadataLoadTask;

import com.codahale.metrics.health.HealthCheck;

import java.util.Set;

import javax.inject.Singleton;

/**
 * Check load statuses of all Druid lookups.
 */
@Singleton
public class LookupHealthCheck extends HealthCheck {
    private final LookupMetadataLoadTask lookupMetadataLoadTask;

    /**
     * Constructor.
     *
     * @param lookupMetadataLoadTask  A {@link LookupMetadataLoadTask} that keeps load statuses of
     * all Druid lookups
     */
    public LookupHealthCheck(LookupMetadataLoadTask lookupMetadataLoadTask) {
        this.lookupMetadataLoadTask = lookupMetadataLoadTask;
    }

    @Override
    public Result check() {
        Set<String> unloadedLookups = lookupMetadataLoadTask.getPendingLookups();
        return unloadedLookups.isEmpty()
                ? Result.healthy("All Druid lookups have been loaded.")
                : Result.unhealthy("Lookups %s are not loaded.", unloadedLookups);
    }
}
