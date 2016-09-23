// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.names;

import com.yahoo.bard.webservice.util.EnumUtils;
import com.yahoo.bard.webservice.util.Utils;

import java.util.Collections;
import java.util.Set;

/**
 * Bard test dimensions.
 */
public enum TestApiDimensionName implements FieldName {
    SIZE,
    SHAPE,
    COLOR,
    MODEL,
    OTHER,
    SPECIES,
    BREED,
    SEX;

    @Override
    public String asName() {
        return EnumUtils.camelCase(name());
    }

    /**
     * Get the set of ApiDimensionNames for the logical table, by name.
     *
     * @param logicalTable  Name of the logical table
     *
     * @return the set of it's dimension names
     */
    public static Set<TestApiDimensionName> getByLogicalTable(TestLogicalTableName logicalTable) {
        switch (logicalTable) {
            case SHAPES:
                return Utils.asLinkedHashSet(SIZE, SHAPE, COLOR, OTHER, MODEL);
            case PETS:
                return Utils.asLinkedHashSet(SPECIES, BREED, SEX);
            case MONTHLY:
            case HOURLY:
            case HOURLY_MONTHLY:
                return Utils.asLinkedHashSet(OTHER);
        }
        return Collections.<TestApiDimensionName>emptySet();
    }
}
