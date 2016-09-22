// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application.healthchecks;

import com.yahoo.bard.webservice.data.dimension.SearchProvider;

import com.codahale.metrics.health.HealthCheck;

/**
 * A health check to test search provider health status.
 */
public class SearchProviderHealthCheck extends HealthCheck {

    private SearchProvider searchProvider;

    /**
     * Constructor.
     *
     * @param searchProvider  SearchProvider to check the health of.
     */
    public SearchProviderHealthCheck(SearchProvider searchProvider) {
        this.searchProvider = searchProvider;
    }

    @Override
    protected Result check() throws Exception {
        if (searchProvider.isHealthy()) {
            return Result.healthy("SearchProvider is healthy.");
        } else {
            return Result.unhealthy("SearchProvider is not healthy.");
        }
    }
}
