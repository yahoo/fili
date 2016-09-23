// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.names;

import java.util.Locale;

/**
 * Names of the test logical tables.
 */
public enum TestLogicalTableName {
    PETS,
    SHAPES,
    HOURLY,
    MONTHLY,
    HOURLY_MONTHLY
    ;

    /**
     * Get the name of the table in an API-visible form.
     *
     * @return the Api name of the table
     */
    public String asName() {
        return this.name().toLowerCase(Locale.getDefault());
    }
}
