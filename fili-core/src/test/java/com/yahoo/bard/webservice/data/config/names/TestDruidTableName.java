// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.names;

import java.util.Locale;

/**
 * Hold the list of raw Druid table names.
 */
public enum TestDruidTableName implements TableName {
    ALL_SHAPES,
    COLOR_SHAPES_HOURLY,
    COLOR_SHAPES,
    COLOR_SHAPES_MONTHLY,
    COLOR_SIZE_SHAPES,
    COLOR_SIZE_SHAPE_SHAPES,
    ALL_PETS,
    MONTHLY,
    HOURLY;

    @Override
    public String asName() {
        return name().toLowerCase(Locale.ENGLISH);
    }
}
