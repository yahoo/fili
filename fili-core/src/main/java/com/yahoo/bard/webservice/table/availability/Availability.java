// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.table.Column;

import org.joda.time.Interval;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

/**
 * Availability describes the intervals available by column for a table.
 */
public interface Availability {

    /**
     * The names of the data sources backing this availability.
     *
     * @return A set of names for datasources backing this table
     */
    SortedSet<TableName> getDataSourceNames();

    /**
     * The availability for a given column.
     *
     * @param c  A column
     *
     * @return The list of intervals that column is available for.
     */
    List<Interval> get(Column c);

    /**
     * The availability of all columns.
     *
     * @return The intervals, by column, available.
     */
    Map<Column, List<Interval>> getAvailableIntervals();

    /**
     * Fetch a set of intervals given a column name.
     *
     * @param columnName  Name of the column
     *
     * @return Set of intervals associated with a column, empty if column is missing
     */
    default List<Interval> getIntervalsByColumnName(String columnName) {
        List<Interval> result = get(new Column(columnName));
        return result == null ? Collections.emptyList() : result;
    }
}
