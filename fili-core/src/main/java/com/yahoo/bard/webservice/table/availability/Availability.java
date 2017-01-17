// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.table.Column;

import org.joda.time.Interval;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Availability describes the intervals available by column for a table.
 */
public interface Availability extends Map<Column, List<Interval>> {

    /**
     * Fetch a set of intervals given a column name.
     *
     * @param columnName  Name of the column
     *
     * @return Set of intervals associated with a column, empty if column is missing
     */
    default List<Interval> getIntervalsByColumnName(String columnName) {
        List<Interval> result = get(new Column(columnName));
        if (result != null) {
            return result;
        }
        return Collections.emptyList();
    }
}
