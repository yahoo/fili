// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql;

import static java.util.Arrays.asList;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DAYOFYEAR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.HOUR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUTE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MONTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.WEEK;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.YEAR;

import com.yahoo.bard.webservice.TimestampUtils;
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import com.yahoo.bard.webservice.druid.model.query.Granularity;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlDatePartFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Handles converting between a {@link DefaultTimeGrain} and a list of
 * {@link SqlDatePartFunction} to create groupBy statements on intervals of time.
 */
public class TimeConverter {
    // This mapping shows what information we need to group for each granularity
    private static final Map<DefaultTimeGrain, List<SqlDatePartFunction>> TIMEGRAIN_TO_GROUPBY = new
            HashMap<DefaultTimeGrain, List<SqlDatePartFunction>>() {{
                put(DefaultTimeGrain.YEAR, asList(YEAR));
                put(DefaultTimeGrain.MONTH, asList(YEAR, MONTH));
                put(DefaultTimeGrain.WEEK, asList(YEAR, WEEK));
                put(DefaultTimeGrain.DAY, asList(YEAR, DAYOFYEAR));
                put(DefaultTimeGrain.HOUR, asList(YEAR, DAYOFYEAR, HOUR));
                put(DefaultTimeGrain.MINUTE, asList(YEAR, DAYOFYEAR, HOUR, MINUTE));
            }};

    /**
     * Private constructor - all methods static.
     */
    private TimeConverter() {

    }

    /**
     * Gets the number of {@link SqlDatePartFunction} used for the given {@link Granularity}.
     *
     * @param granularity  The timegrain to find groupBy functions for.
     *
     * @return the number of functions used to groupBy the given granularity.
     */
    public static int getNumberOfGroupByFunctions(Granularity granularity) {
        return TIMEGRAIN_TO_GROUPBY.get(granularity).size();
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
    public static Stream<RexNode> buildGroupBy(
            RelBuilder builder,
            Granularity granularity,
            String timeColumn
    ) {
        return TIMEGRAIN_TO_GROUPBY.get(granularity)
                .stream()
                .map(sqlDatePartFunction -> builder.call(sqlDatePartFunction, builder.field(timeColumn)));
    }

    /**
     * Given a {@link ResultSet} and the {@link Granularity} used to make groupBy
     * statements on time, it will parse out a {@link DateTime} for one row which
     * represents the beginning of the interval it was grouped on.
     *
     *
     * @param offset the last column before the date fields.
     * @param row  The results returned by Sql needed to read the time columns.
     * @param granularity  The granularity which was used when calling
     * {@link #buildGroupBy(RelBuilder, Granularity, String)}
     *
     * @return the datetime for the start of the interval.
     */
    public static DateTime parseDateTime(int offset, String[] row, Granularity granularity) {
        DefaultTimeGrain timeGrain = (DefaultTimeGrain) granularity;
        DateTime resultTimeStamp = new DateTime(0, DateTimeZone.UTC);

        List<SqlDatePartFunction> times = TIMEGRAIN_TO_GROUPBY.get(timeGrain);
        for (int i = 0; i < times.size(); i++) {
            int value = Integer.parseInt(row[offset + i]);
            SqlDatePartFunction fn = times.get(i);
            resultTimeStamp = setDateTime(value, fn, resultTimeStamp);
        }

        return resultTimeStamp;
    }

    /**
     * Sets the correct part of a {@link DateTime} corresponding to a
     * {@link SqlDatePartFunction}.
     *
     * @param value  The value to be set for the dateTime with the sqlDatePartFn
     * @param sqlDatePartFn  The function used to extract part of a date with sql.
     * @param dateTime  The original dateTime to create a copy of.
     *
     * @return the dateTime with a modified value corresponding to the sqlDatePartFn.
     */
    private static DateTime setDateTime(int value, SqlDatePartFunction sqlDatePartFn, DateTime dateTime) {
        if (sqlDatePartFn.equals(YEAR)) {
            return dateTime.withYear(value);
        } else if (sqlDatePartFn.equals(MONTH)) {
            return dateTime.withMonthOfYear(value);
        } else if (sqlDatePartFn.equals(WEEK)) {
            return dateTime.withWeekOfWeekyear(value);
        } else if (sqlDatePartFn.equals(DAYOFYEAR)) {
            return dateTime.withDayOfYear(value);
        } else if (sqlDatePartFn.equals(HOUR)) {
            return dateTime.withHourOfDay(value);
        } else if (sqlDatePartFn.equals(MINUTE)) {
            return dateTime.withMinuteOfHour(value);
        } else {
            throw new IllegalArgumentException("Can't parse SqlDatePartFunction");
        }
    }

    /**
     * Builds the time filters to only select rows that occur within the intervals of the query.
     * NOTE: you must have one interval to select on.
     *
     * @param builder  The RelBuilder used for building queries.
     * @param intervals  The intervals to select from.
     * @param timestampColumn  The name of the timestamp column in the database.
     *
     * @return the RexNode for filtering to only the given intervals.
     */
    public static RexNode buildTimeFilters(
            RelBuilder builder,
            Collection<Interval> intervals,
            String timestampColumn
    ) {
        // create filters to only select results within the given intervals
        List<RexNode> timeFilters = intervals.stream()
                .map(interval -> {
                    Timestamp start = TimestampUtils.timestampFromMillis(interval.getStartMillis());
                    Timestamp end = TimestampUtils.timestampFromMillis(interval.getEndMillis());

                    return builder.and(
                            builder.call(
                                    SqlStdOperatorTable.GREATER_THAN,
                                    builder.field(timestampColumn),
                                    builder.literal(start.toString())
                            ),
                            builder.call(
                                    SqlStdOperatorTable.LESS_THAN,
                                    builder.field(timestampColumn),
                                    builder.literal(end.toString())
                            )
                    );
                })
                .collect(Collectors.toList());

        return builder.or(timeFilters);
    }
}
