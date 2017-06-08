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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlDatePartFunction;
import org.apache.calcite.tools.RelBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by hinterlong on 6/7/17.
 */
public class TimeConverter {
    private static final BiMap<DefaultTimeGrain, List<SqlDatePartFunction>> TIMEGRAIN_TO_GROUPBY = HashBiMap.create(6);
    //todo create specific sets of filters,

    /**
     * This mapping shows what information we need to group for each granularity
     * Year   -> (Year)
     * Month  -> (Year, Month)
     * Week   -> (Year, Week)
     * Day    -> (Year, Day)
     * Hour   -> (Year, Day, Hour)
     * Minute -> (Year, Day, Hour, Minute)
     */
    static {
        TIMEGRAIN_TO_GROUPBY.put(DefaultTimeGrain.YEAR, asList(YEAR));
        TIMEGRAIN_TO_GROUPBY.put(DefaultTimeGrain.MONTH, asList(YEAR, MONTH));
        TIMEGRAIN_TO_GROUPBY.put(DefaultTimeGrain.WEEK, asList(YEAR, WEEK));
        TIMEGRAIN_TO_GROUPBY.put(DefaultTimeGrain.DAY, asList(YEAR, DAYOFYEAR));
        TIMEGRAIN_TO_GROUPBY.put(DefaultTimeGrain.HOUR, asList(YEAR, DAYOFYEAR, HOUR));
        TIMEGRAIN_TO_GROUPBY.put(DefaultTimeGrain.MINUTE, asList(YEAR, DAYOFYEAR, HOUR, MINUTE));
    }

    private TimeConverter() {

    }

    public static List<SqlDatePartFunction> getDatePartFunctions(Granularity granularity) {
        DefaultTimeGrain timeGrain = (DefaultTimeGrain) granularity;

        return TIMEGRAIN_TO_GROUPBY.get(timeGrain);
    }

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
