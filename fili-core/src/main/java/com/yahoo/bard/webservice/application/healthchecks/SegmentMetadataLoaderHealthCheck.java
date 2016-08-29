// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.application.healthchecks;

import com.yahoo.bard.webservice.metadata.SegmentMetadataLoader;

import com.codahale.metrics.health.HealthCheck;

import org.joda.time.DateTime;

import javax.inject.Singleton;

/**
 * Check to verify if the loader runs as scheduled.
 *
 * @deprecated The endpoints in Druid that the SegmentMetadataLoader relies on have been deprecated.
 */
@Deprecated
@Singleton
public class SegmentMetadataLoaderHealthCheck extends HealthCheck {

    private final SegmentMetadataLoader loader;
    private final long lastRunDuration;

    /**
     * Constructor.
     *
     * @param loader  segment metadata loader
     * @param lastRunDuration  last run duration
     */
    public SegmentMetadataLoaderHealthCheck(SegmentMetadataLoader loader, long lastRunDuration) {
        this.loader = loader;
        this.lastRunDuration = lastRunDuration;
    }

    @Override
    public Result check() throws Exception {
        // check if loader ran within the lastRunDuration (i.e. X milliseconds ago)
        if (loader.getLastRunTimestamp().isAfter(DateTime.now().minus(lastRunDuration))) {
            return Result.healthy("Segment metadata loader is healthy, last run: %s.", loader.getLastRunTimestamp());
        }
        return Result.unhealthy("Segment metadata loader is not running, last run: %s.", loader.getLastRunTimestamp());
    }
}
