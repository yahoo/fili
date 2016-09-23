// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application.healthchecks;

import com.yahoo.bard.webservice.data.dimension.KeyValueStore;

import com.codahale.metrics.health.HealthCheck;

/**
 * A health check to test if a keyValueStore reports as healthy.
 */
public class KeyValueStoreHealthCheck extends HealthCheck {

    private final KeyValueStore keyValueStore;

    /**
     * Constructor.
     *
     * @param keyValueStore  KeyValueStore to check the health of.
     */
    public KeyValueStoreHealthCheck(KeyValueStore keyValueStore) {
        this.keyValueStore = keyValueStore;
    }

    @Override
    public Result check() {
        if (keyValueStore.isHealthy()) {
            return Result.healthy("KeyValueStore is healthy.");
        } else {
            return Result.unhealthy("KeyValueStore has been corrupted.");
        }
    }
}
