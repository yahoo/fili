// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.names;

import com.yahoo.bard.webservice.util.Utils;

import java.util.Collections;
import java.util.Locale;
import java.util.Set;

/**
 * Hold the list of raw Druid metric names.
 */
public enum TestDruidMetricName implements FieldName {
    HEIGHT,
    WIDTH,
    DEPTH,
    USERS,
    LIMBS;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ENGLISH);
    }

    @Override
    public String asName() {
        return toString();
    }

    /**
     * Get the Druid Metric Names by the logical table they should be present in.
     *
     * @param logicalTable  Logical table for which to get the DruidMetricNames
     *
     * @return the Druid metric names for that logical table
     */
    public static Set<FieldName> getByLogicalTable(TestLogicalTableName logicalTable) {
        switch (logicalTable) {
            case SHAPES:
                return Utils.asLinkedHashSet(HEIGHT, WIDTH, DEPTH, USERS);
            case PETS:
                return Utils.asLinkedHashSet(LIMBS);
            case MONTHLY:
            case HOURLY:
                return Utils.asLinkedHashSet(LIMBS);
        }
        return Collections.<FieldName>emptySet();
    }
}
