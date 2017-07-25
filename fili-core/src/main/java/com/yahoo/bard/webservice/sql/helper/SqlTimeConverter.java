// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.helper;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DAYOFYEAR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.HOUR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUTE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MONTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SECOND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.WEEK;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.YEAR;

import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.Granularity;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlDatePartFunction;
import org.apache.calcite.tools.RelBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Provides the ability to GroupBy, Filter on, and parse date time
 * information from a sql backend.
 */
public interface SqlTimeConverter {

    /**
     * Gets a list of {@link SqlDatePartFunction} to be performed on a timestamp
     * which can be used to group by the given {@link Granularity}.
     *
     * @param granularity  The granularity to map to a list of {@link SqlDatePartFunction}.
     *
     * @return the list of sql functions.
     */
    List<SqlDatePartFunction> timeGrainToDatePartFunctions(Granularity granularity);

    /**
     * Builds the time filters to only select rows that occur within the intervals of the query.
     * NOTE: you must have one interval to select on.
     *
     * @param builder  The RelBuilder used for building queries.
     * @param druidQuery  The druid query to build filters over.
     * @param timestampColumn  The name of the timestamp column in the database.
     *
     * @return the RexNode for filtering to only the given intervals.
     */
    RexNode buildTimeFilters(RelBuilder builder, DruidAggregationQuery<?> druidQuery, String timestampColumn);

    /**
     * Gets the number of {@link SqlDatePartFunction} used for the given {@link Granularity}.
     *
     * @param granularity  The timegrain to find groupBy functions for.
     *
     * @return the number of functions used to groupBy the given granularity.
     */
    default int getNumberOfGroupByFunctions(Granularity granularity) {
        return timeGrainToDatePartFunctions(granularity).size();
    }

    /**
     * Builds a list of {@link RexNode} which will effectively groupBy the given {@link Granularity}.
     *
     * @param builder  The RelBuilder used with calcite to build queries.
     * @param granularity  The granularity to build the groupBy for.
     * @param timeColumn  The name of the timestamp column.
     *
     * @return the list of {@link RexNode} needed in the groupBy.
     */
    default Stream<RexNode> buildGroupBy(RelBuilder builder, Granularity granularity, String timeColumn) {
        List<SqlDatePartFunction> sqlDatePartFunctions = timeGrainToDatePartFunctions(granularity);
        if (sqlDatePartFunctions.isEmpty()) {
            return Stream.of(builder.field(timeColumn));
        }

        return sqlDatePartFunctions
                .stream()
                .map(sqlDatePartFunction -> builder.call(sqlDatePartFunction, builder.field(timeColumn)));
    }

    /**
     * Given an array of strings (a row from a {@link java.sql.ResultSet}) and the
     * {@link Granularity} used to make groupBy statements on time, it will parse out a {@link DateTime}
     * for the row which represents the beginning of the interval it was grouped on.
     *
     * @param offset the last column before the date fields.
     * @param row  The results returned by Sql needed to read the time columns.
     * @param druidQuery  The original druid query which was made using calling
     * {@link #buildGroupBy(RelBuilder, Granularity, String)}.
     *
     * @return the datetime for the start of the interval.
     */
    default DateTime getIntervalStart(int offset, String[] row, DruidAggregationQuery<?> druidQuery) {
        List<SqlDatePartFunction> times = timeGrainToDatePartFunctions(druidQuery.getGranularity());

        DateTimeZone timeZone = getTimeZone(druidQuery);

        if (times.isEmpty()) {
            Timestamp timestamp = Timestamp.valueOf(row[offset]);
            ZonedDateTime zonedDateTime = timestamp.toLocalDateTime().atZone(ZoneId.of(timeZone.getID()));
            return new DateTime(TimeUnit.SECONDS.toMillis(zonedDateTime.toEpochSecond()), timeZone);
        }

        MutableDateTime mutableDateTime = new MutableDateTime(0, 1, 1, 0, 0, 0, 0, timeZone);

        for (int i = 0; i < times.size(); i++) {
            int value = Integer.parseInt(row[offset + i]);
            SqlDatePartFunction fn = times.get(i);
            setDateTime(value, fn, mutableDateTime);
        }

        return mutableDateTime.toDateTime();
    }

    /**
     * Gets the timezone of the backing table for the given druid query.
     *
     * @param druidQuery  The druid query to find the timezone for
     *
     * @return the {@link DateTimeZone} of the physical table for this query.
     */
    default DateTimeZone getTimeZone(DruidAggregationQuery<?> druidQuery) {
        return druidQuery.getDataSource()
                .getPhysicalTable()
                .getSchema()
                .getTimeGrain()
                .getTimeZone();
    }

    /**
     * Sets the correct part of a {@link DateTime} corresponding to a
     * {@link SqlDatePartFunction}.
     *
     * @param value  The value to be set for the dateTime with the sqlDatePartFn
     * @param sqlDatePartFn  The function used to extract part of a date with sql.
     * @param dateTime  The original dateTime to create a copy of.
     */
    default void setDateTime(int value, SqlDatePartFunction sqlDatePartFn, MutableDateTime dateTime) {
        if (YEAR.equals(sqlDatePartFn)) {
            dateTime.setYear(value);
        } else if (MONTH.equals(sqlDatePartFn)) {
            dateTime.setMonthOfYear(value);
        } else if (WEEK.equals(sqlDatePartFn)) {
            dateTime.setWeekOfWeekyear(value);
            dateTime.setDayOfWeek(1);
        } else if (DAYOFYEAR.equals(sqlDatePartFn)) {
            dateTime.setDayOfYear(value);
        } else if (HOUR.equals(sqlDatePartFn)) {
            dateTime.setHourOfDay(value);
        } else if (MINUTE.equals(sqlDatePartFn)) {
            dateTime.setMinuteOfHour(value);
        } else if (SECOND.equals(sqlDatePartFn)) {
            dateTime.setSecondOfMinute(value);
        } else {
            throw new IllegalArgumentException("Can't set value " + value + " for " + sqlDatePartFn);
        }
    }
}
