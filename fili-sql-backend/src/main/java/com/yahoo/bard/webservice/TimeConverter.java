// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice;

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import com.yahoo.bard.webservice.druid.model.query.Granularity;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by hinterlong on 6/7/17.
 */
public class TimeConverter {
    private static final BiMap<DefaultTimeGrain, Integer> TIMEGRAIN_TO_INDEX = HashBiMap.create(6);
    //todo create specific sets of filters, (Year) (Year, month) (Year, week) (Year, day) (Year, day, hour) (year, day, hour, minute)
    static {
        TIMEGRAIN_TO_INDEX.put(DefaultTimeGrain.YEAR, 1);
        TIMEGRAIN_TO_INDEX.put(DefaultTimeGrain.MONTH, 2);
        TIMEGRAIN_TO_INDEX.put(DefaultTimeGrain.WEEK, 3);
        TIMEGRAIN_TO_INDEX.put(DefaultTimeGrain.DAY, 4);
        TIMEGRAIN_TO_INDEX.put(DefaultTimeGrain.HOUR, 5);
        TIMEGRAIN_TO_INDEX.put(DefaultTimeGrain.MINUTE, 6);
    }

    private TimeConverter() {

    }

    public static int getTimeGranularity(Granularity granularity) {
        if (!(granularity instanceof DefaultTimeGrain)) {
            throw new IllegalStateException("Must be a DefaultTimeGrain");
        }
        DefaultTimeGrain timeGrain = (DefaultTimeGrain) granularity;

        return TIMEGRAIN_TO_INDEX.get(timeGrain);
    }

    public static List<RexNode> getGroupByForGranularity(RelBuilder builder, Granularity granularity, String timeColumnumn) {
        return getGroupByForGranularity(builder, getTimeGranularity(granularity), timeColumnumn);
    }

    public static List<RexNode> getGroupByForGranularity(RelBuilder builder, int index, String timeColumn) {
        List<RexNode> times = Arrays.asList(
                builder.call(SqlStdOperatorTable.YEAR, builder.field(timeColumn)),
                builder.call(SqlStdOperatorTable.MONTH, builder.field(timeColumn)),
                builder.call(SqlStdOperatorTable.WEEK, builder.field(timeColumn)),
                builder.call(SqlStdOperatorTable.DAYOFYEAR, builder.field(timeColumn)),
                builder.call(SqlStdOperatorTable.HOUR, builder.field(timeColumn)),
                builder.call(SqlStdOperatorTable.MINUTE, builder.field(timeColumn))
        );
        return times.subList(0, index);
    }

    public static DateTime getDateTime(ResultSet resultSet, int timeGranularity) throws SQLException {
        DateTime resultTimeStamp = new DateTime(DateTimeZone.UTC);

        Integer yearIndex = TIMEGRAIN_TO_INDEX.get(DefaultTimeGrain.YEAR);
        if (timeGranularity >= yearIndex) {
            int year = resultSet.getInt(yearIndex);
            resultTimeStamp = resultTimeStamp.withYear(year);
        }

        Integer monthIndex = TIMEGRAIN_TO_INDEX.get(DefaultTimeGrain.MONTH);
        if (timeGranularity >= monthIndex) {
            int month = resultSet.getInt(monthIndex);
            resultTimeStamp = resultTimeStamp.withMonthOfYear(month);
        } else {
            resultTimeStamp = resultTimeStamp.withMonthOfYear(1);
        }

        Integer weekIndex = TIMEGRAIN_TO_INDEX.get(DefaultTimeGrain.WEEK);
        if (timeGranularity >= weekIndex) {
            int weekOfYear = resultSet.getInt(weekIndex);
            resultTimeStamp = resultTimeStamp.withWeekOfWeekyear(weekOfYear);
        } else {
            resultTimeStamp = resultTimeStamp.withWeekOfWeekyear(1);
        }

        Integer dayIndex = TIMEGRAIN_TO_INDEX.get(DefaultTimeGrain.DAY);
        if (timeGranularity >= dayIndex) {
            int dayOfYear = resultSet.getInt(dayIndex);
            resultTimeStamp = resultTimeStamp.withDayOfYear(dayOfYear);
        } else {
            resultTimeStamp = resultTimeStamp.withDayOfYear(1);
        }

        Integer hourIndex = TIMEGRAIN_TO_INDEX.get(DefaultTimeGrain.HOUR);
        if (timeGranularity >= hourIndex) {
            int hourOfDay = resultSet.getInt(hourIndex);
            resultTimeStamp = resultTimeStamp.withHourOfDay(hourOfDay);
        } else {
            resultTimeStamp = resultTimeStamp.withHourOfDay(0);
        }

        Integer minuteIndex = TIMEGRAIN_TO_INDEX.get(DefaultTimeGrain.MINUTE);
        if (timeGranularity >= minuteIndex) {
            int minuteOfHour = resultSet.getInt(minuteIndex);
            resultTimeStamp = resultTimeStamp.withMinuteOfHour(minuteOfHour);
        } else {
            resultTimeStamp = resultTimeStamp.withMinuteOfHour(0);
        }

        resultTimeStamp = resultTimeStamp.withSecondOfMinute(0).withMillisOfSecond(0);
        return resultTimeStamp;
    }
}
