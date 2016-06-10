// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.names;

import java.util.Locale;

public enum TestLogicalTableName {
    PETS,
    SHAPES,
    HOURLY,
    MONTHLY;

    public String asName() {
        return this.name().toLowerCase(Locale.getDefault());
    }
}
