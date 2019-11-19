// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application.healthchecks;

import com.yahoo.bard.webservice.application.DimensionValueLoadTask;

import com.codahale.metrics.health.HealthCheck;

import org.joda.time.DateTime;

import javax.inject.Singleton;

/**
 * Check to verify if the loader runs as scheduled.
 */
@Singleton
public class DruidDimensionsLoaderHealthCheck extends HealthCheck {

    private final DimensionValueLoadTask loader;
    private final long lastRunDuration;

    /**
     * Constructor.
     *
     * @param loader  segment metadata loader
     * @param lastRunDuration  last run duration
     */
    public DruidDimensionsLoaderHealthCheck(DimensionValueLoadTask loader, long lastRunDuration) {
        this.loader = loader;
        this.lastRunDuration = lastRunDuration;
    }

    @Override
    public Result check() throws Exception {
        // check if loader ran within the lastRunDuration (i.e. X milliseconds ago)
        if (loader.getLastRunTimestamp() == null) {
            return Result.unhealthy(
                    "Druid dimension loader has not yet run.",
                    loader.getLastRunTimestamp()
            );
        }
        if (loader.getLastRunTimestamp().isAfter(DateTime.now().minus(lastRunDuration))) {
            return Result.healthy("Druid dimensions loader is healthy, last run: %s.", loader.getLastRunTimestamp());
        }
        return Result.unhealthy(
                "Druid dimensions loader is not running, last run: %s.",
                loader.getLastRunTimestamp()
        );
    }
}
