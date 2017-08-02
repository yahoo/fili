// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import javax.inject.Singleton;

/**
 * DruidDimensionsLoader sends requests to the druid search query interface to get a list of dimension
 * values to add to the dimension cache.
 */
@Singleton
public class DruidDimensionsLoader extends Loader<Boolean>
        implements BiConsumer<Dimension, DimensionRow>, Consumer<Dimension> {
    private static final Logger LOG = LoggerFactory.getLogger(DruidDimensionsLoader.class);

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    public static final String DRUID_DIM_LOADER_TIMER_DURATION_KEY =
            SYSTEM_CONFIG.getPackageVariableName("druid_dim_loader_timer_duration");
    public static final String DRUID_DIM_LOADER_TIMER_DELAY_KEY =
            SYSTEM_CONFIG.getPackageVariableName("druid_dim_loader_timer_delay");

    private final AtomicReference<DateTime> lastRunTimestamp;
    private final Collection<AbstractDimensionRowProvider> dimensionRowProviders;

    /**
     * DruidDimensionsLoader fetches data from Druid and adds it to the dimension cache.
     * The dimensions to be loaded can be passed in as a parameter.
     *
     * @param dimensionRowProviders  A set of {@link AbstractDimensionRowProvider} to initialize dimensions.
     */
    public DruidDimensionsLoader(Collection<AbstractDimensionRowProvider> dimensionRowProviders) {
        super(
                DruidDimensionsLoader.class.getSimpleName(),
                SYSTEM_CONFIG.getLongProperty(DRUID_DIM_LOADER_TIMER_DELAY_KEY, 0),
                SYSTEM_CONFIG.getLongProperty(
                        DRUID_DIM_LOADER_TIMER_DURATION_KEY,
                        TimeUnit.MINUTES.toMillis(1)
                )
        );

        this.dimensionRowProviders = dimensionRowProviders;
        lastRunTimestamp = new AtomicReference<>();

        HttpErrorCallback errorCallback = getErrorCallback();
        FailureCallback failureCallback = getFailureCallback();

        dimensionRowProviders.forEach(dimensionRowProvider -> {
            dimensionRowProvider.setDimensionRowConsumer(this);
            dimensionRowProvider.setDimensionLoadedConsumer(this);
            dimensionRowProvider.setErrorCallback(errorCallback);
            dimensionRowProvider.setFailureCallback(failureCallback);
        });
    }


    @Override
    public void run() {
        dimensionRowProviders.forEach(AbstractDimensionRowProvider::load);
        // tell all dimensionRowProviders to load
        lastRunTimestamp.set(DateTime.now());
    }

    public DateTime getLastRunTimestamp() {
        return lastRunTimestamp.get();
    }

    @Override
    public void accept(Dimension dimension, DimensionRow dimensionRow) {
        dimension.addDimensionRow(dimensionRow);
    }

    @Override
    public void accept(Dimension dimension) {
        // Tell the dimension it's been updated
        dimension.setLastUpdated(DateTime.now());
    }
}
