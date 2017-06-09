// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice;

import static java.util.Arrays.asList;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DAYOFYEAR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.HOUR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUTE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MONTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.WEEK;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.YEAR;

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import com.yahoo.bard.webservice.druid.model.query.Granularity;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlDatePartFunction;
import org.apache.calcite.tools.RelBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Handles converting between a {@link DefaultTimeGrain} and a list of
 * {@link SqlDatePartFunction} to create groupBy statements on intervals of time.
 */
public class TimeConverter {
    private static final Map<DefaultTimeGrain, List<SqlDatePartFunction>> TIMEGRAIN_TO_GROUPBY = new HashMap<>();

    /*
      This mapping shows what information we need to group for each granularity
      Year   -> (Year)
      Month  -> (Year, Month)
      Week   -> (Year, Week)
      Day    -> (Year, Day)
      Hour   -> (Year, Day, Hour)
      Minute -> (Year, Day, Hour, Minute)
     */
    static {
        TIMEGRAIN_TO_GROUPBY.put(DefaultTimeGrain.YEAR, asList(YEAR));
        TIMEGRAIN_TO_GROUPBY.put(DefaultTimeGrain.MONTH, asList(YEAR, MONTH));
        TIMEGRAIN_TO_GROUPBY.put(DefaultTimeGrain.WEEK, asList(YEAR, WEEK));
        TIMEGRAIN_TO_GROUPBY.put(DefaultTimeGrain.DAY, asList(YEAR, DAYOFYEAR));
        TIMEGRAIN_TO_GROUPBY.put(DefaultTimeGrain.HOUR, asList(YEAR, DAYOFYEAR, HOUR));
        TIMEGRAIN_TO_GROUPBY.put(DefaultTimeGrain.MINUTE, asList(YEAR, DAYOFYEAR, HOUR, MINUTE));
    }

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
        DefaultTimeGrain timeGrain = (DefaultTimeGrain) granularity;
        return TIMEGRAIN_TO_GROUPBY.get(timeGrain).size();
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
    public static List<RexNode> buildGroupBy(
            RelBuilder builder,
            Granularity granularity,
            String timeColumn
    ) {
        DefaultTimeGrain timeGrain = (DefaultTimeGrain) granularity;
        return TIMEGRAIN_TO_GROUPBY.get(timeGrain)
                .stream()
                .map(sqlDatePartFunction -> builder.call(sqlDatePartFunction, builder.field(timeColumn)))
                .collect(Collectors.toList());
    }

    /**
     * Given a {@link ResultSet} and the {@link Granularity} used to make groupBy
     * statements on time, it will parse out a {@link DateTime} for one row which
     * represents the beginning of the interval it was grouped on.
     *
     * @param resultSet  The results returned by Sql needed to read the time columns.
     * @param granularity  The granularity which was used when calling
     * {@link #buildGroupBy(RelBuilder, Granularity, String)}
     *
     * @return the datetime for the start of the interval.
     *
     * @throws SQLException if the results can't be read.
     */
    public static DateTime parseDateTime(ResultSet resultSet, Granularity granularity) throws SQLException {
        DefaultTimeGrain timeGrain = (DefaultTimeGrain) granularity;
        DateTime resultTimeStamp = new DateTime(DateTimeZone.UTC);

        resultTimeStamp = resultTimeStamp.withMonthOfYear(1);
        resultTimeStamp = resultTimeStamp.withWeekOfWeekyear(1);
        resultTimeStamp = resultTimeStamp.withDayOfYear(1);
        resultTimeStamp = resultTimeStamp.withHourOfDay(0);
        resultTimeStamp = resultTimeStamp.withMinuteOfHour(0);
        resultTimeStamp = resultTimeStamp.withSecondOfMinute(0).withMillisOfSecond(0);

        List<SqlDatePartFunction> times = TIMEGRAIN_TO_GROUPBY.get(timeGrain);
        for (int i = 1; i <= times.size(); i++) {
            int value = resultSet.getInt(i);
            SqlDatePartFunction fn = times.get(i - 1);
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
}
