// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

/**
 * Enumerates the list of keys expected to be found in the ResponseContext.
 */
public enum ResponseContextKeys {
    MISSING_INTERVALS_CONTEXT_KEY("missingIntervals"),
    VOLATILE_INTERVALS_CONTEXT_KEY("volatileIntervals"),
    PAGINATION_LINKS_CONTEXT_KEY("paginationLinks"),
    PAGINATION_CONTEXT_KEY("pagination"),
    API_METRIC_COLUMN_NAMES("apiMetricColumnNames"),
    REQUESTED_API_DIMENSION_FIELDS("requestedApiDimensionFields"),
    STATUS("status"),
    ERROR_MESSAGE("errorMessage"),
    HEADERS("headers");

    private final String name;

    /**
     * Constructor.
     *
     * @param name  Name of the context key
     */
    ResponseContextKeys(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
