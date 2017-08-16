// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application.healthchecks;

import com.yahoo.bard.webservice.metadata.DataSourceMetadataLoadTask;

import com.codahale.metrics.health.HealthCheck;

import org.joda.time.DateTime;

import javax.inject.Singleton;

/**
 * Check to verify if the loader runs as scheduled.
 */
@Singleton
public class DataSourceMetadataLoaderHealthCheck extends HealthCheck {

    private final DataSourceMetadataLoadTask loader;
    private final long executionWindow;

    /**
     * Creates a health check for a datasource metadata loader.
     *
     * @param loader  Datasource metadata loader.
     * @param executionWindow  The duration within which a loader must run successfully to be considered healthy.
     */
    public DataSourceMetadataLoaderHealthCheck(DataSourceMetadataLoadTask loader, long executionWindow) {
        this.loader = loader;
        this.executionWindow = executionWindow;
    }

    @Override
    public Result check() throws Exception {
        // check if loader ran within the lastRunDuration (i.e. X milliseconds ago)
        if (loader.getLastRunTimestamp().isAfter(DateTime.now().minus(executionWindow))) {
            return Result.healthy("Datasource metadata loader is healthy, last run: %s.", loader.getLastRunTimestamp());
        }
        return Result.unhealthy(
                "Datasource metadata loader is not running, last run: %s.",
                loader.getLastRunTimestamp()
        );
    }
}
