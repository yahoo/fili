// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.names;

import com.yahoo.bard.webservice.util.Utils;

import java.util.Collections;
import java.util.Locale;
import java.util.Set;

/**
 * Hold the list of raw Druid table names.
 */
public enum TestDruidTableName implements TableName {
    ALL_SHAPES,
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

    public Set<TestDruidTableName> getByLogicalTable(TestLogicalTableName logicalTableName) {
        switch (logicalTableName) {
            case SHAPES:
                return Utils.asLinkedHashSet(
                        ALL_SHAPES,
                        COLOR_SHAPES,
                        COLOR_SHAPES_MONTHLY,
                        COLOR_SIZE_SHAPES,
                        COLOR_SIZE_SHAPE_SHAPES
                );
            case PETS:
                return Utils.asLinkedHashSet(ALL_PETS);
            case MONTHLY:
                return Utils.asLinkedHashSet(MONTHLY);
            case HOURLY:
                return Utils.asLinkedHashSet(HOURLY);
        }
        return Collections.<TestDruidTableName>emptySet();
    }
}
