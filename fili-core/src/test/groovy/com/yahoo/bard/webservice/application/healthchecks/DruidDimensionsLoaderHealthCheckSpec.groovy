// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application.healthchecks


import static com.yahoo.bard.webservice.config.BardFeatureFlag.DRUID_DIMENSIONS_LOADER

import com.yahoo.bard.webservice.application.DruidDimensionsLoader

import org.joda.time.DateTime

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll


class DruidDimensionsLoaderHealthCheckSpec extends Specification {
    private static final long TWO_MINUTES = 2 * 60 * 1000;

    /**
     * Setup loader health check and lastRunTimestamp on DruidDimensionsLoader.
     * lastRunTimestamp = current time - timeToSubtract
     *
     * @param timeToSubtract The number of milliseconds to subtract from current time.
     * @param window The window to configure the loader with.
     *
     * @return DruidDimensionsLoaderHealthCheck object
     */
    DruidDimensionsLoaderHealthCheck setupLoaderHealthCheck(long timeToSubtract, long window) {
        DruidDimensionsLoader loader = Mock(DruidDimensionsLoader.class)
        loader.getLastRunTimestamp() >> { return DateTime.now().minus(timeToSubtract)}
        new DruidDimensionsLoaderHealthCheck(loader, window)
    }

    @Shared boolean loaderStatus

    def setupSpec() {
        loaderStatus = DRUID_DIMENSIONS_LOADER.isOn();
        DRUID_DIMENSIONS_LOADER.setOn(true)
    }

    def cleanupSpec() {
        DRUID_DIMENSIONS_LOADER.setOn(loaderStatus)
    }

    @Unroll
    def "Loader that ran #msAgo is #healthy when window is #window"() {
        expect:
        setupLoaderHealthCheck(msAgo, window).check().isHealthy() == isHealthy

        where:
        msAgo         || isHealthy
        30 * 1000     || true
        60 * 1000     || true
        3 * 30 * 1000 || true
        2 * 60 * 1000 || false
        5 * 30 * 1000 || false

        window = TWO_MINUTES
        healthy = isHealthy ? "healthy" : "not healthy"
    }
}
