// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;

import org.joda.time.DateTime;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Singleton;

/**
 * DimensionLoaderTask takes a collection of {@link DimensionValueLoader} to update the values for dimensions.
 */
@Singleton
public class DimensionValueLoadTask extends LoadTask<Boolean> {
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    public static final String LOADER_TIMER_DURATION =
            SYSTEM_CONFIG.getPackageVariableName("druid_dim_loader_timer_duration");
    public static final String LOADER_TIMER_DELAY =
            SYSTEM_CONFIG.getPackageVariableName("druid_dim_loader_timer_delay");

    private final AtomicReference<DateTime> lastRunTimestamp;
    private final Collection<DimensionValueLoader> dimensionRowProviders;

    /**
     * DimensionLoaderTask tells all of the {@link DimensionValueLoader}s to update and add values to the dimension
     * cache.
     *
     * @param dimensionRowProviders  A Collection of {@link DimensionValueLoader} to initialize dimensions.
     */
    public DimensionValueLoadTask(Collection<DimensionValueLoader> dimensionRowProviders) {
        super(
                DimensionValueLoadTask.class.getSimpleName(),
                SYSTEM_CONFIG.getLongProperty(LOADER_TIMER_DELAY, 0),
                SYSTEM_CONFIG.getLongProperty(
                        LOADER_TIMER_DURATION,
                        TimeUnit.MINUTES.toMillis(1)
                )
        );

        this.dimensionRowProviders = dimensionRowProviders;
        lastRunTimestamp = new AtomicReference<>();

        HttpErrorCallback errorCallback = getErrorCallback();
        FailureCallback failureCallback = getFailureCallback();

        dimensionRowProviders.forEach(dimensionRowProvider -> {
            dimensionRowProvider.setErrorCallback(errorCallback);
            dimensionRowProvider.setFailureCallback(failureCallback);
        });
    }

    @Override
    public void runInner() {
        dimensionRowProviders.forEach(DimensionValueLoader::load);
        // tell all dimensionRowProviders to load
        lastRunTimestamp.set(DateTime.now());
    }

    public DateTime getLastRunTimestamp() {
        return lastRunTimestamp.get();
    }
}
