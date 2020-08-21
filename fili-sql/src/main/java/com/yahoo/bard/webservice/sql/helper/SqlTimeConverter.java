// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.helper;

import static java.util.Arrays.asList;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DAYOFYEAR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.HOUR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUTE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MONTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SECOND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.WEEK;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.YEAR;

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.data.time.AllGranularity;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.data.time.Granularity;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlDatePartFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Handles converting between a {@link DefaultTimeGrain} and a list of
 * {@link SqlDatePartFunction} to create groupBy statements on intervals of time.
 */
public class SqlTimeConverter {
    // This mapping shows what information we need to group for each granularity
    private final Map<Granularity, List<SqlDatePartFunction>> granularityToDateFunctionMap;
    private final String timeStringFormat;

    /**
     * Builds a sql time converter which can group by, filter, and reparse dateTimes from a row in a ResultSet using the
     * {@link #buildDefaultGranularityToDateFunctionsMap()} map. Use the default "yyyy-MM-dd HH:mm:ss.S" as time string
     * format
     */
    public SqlTimeConverter() {
        this(buildDefaultGranularityToDateFunctionsMap(), "yyyy-MM-dd HH:mm:ss.S");
    }

    /**
     * Builds a sql time converter which can group by, filter, and reparse dateTimes from a row in a ResultSet using the
     * {@link #buildDefaultGranularityToDateFunctionsMap()} map and set the timestamp format.
     * @param timeStringFormat The time string format
     */
    public SqlTimeConverter(String timeStringFormat) {
        this(buildDefaultGranularityToDateFunctionsMap(), timeStringFormat);
    }

    /**
     * Builds a sql time converter which can group by, filter, and reparse dateTimes from a row in a ResultSet.
     *
     * @param granularityToDateFunctionMap  The mapping defining what granularity needs to be kept in order to properly
     * group by and reparse a dateTime.
     * @param timeStringFormat The time string format for the timestamp column
     */
    public SqlTimeConverter(
            Map<Granularity, List<SqlDatePartFunction>> granularityToDateFunctionMap,
            String timeStringFormat
    ) {
        this.granularityToDateFunctionMap = granularityToDateFunctionMap;
        this.timeStringFormat = timeStringFormat;
    }

    /**
     * Builds the default mapping between {@link Granularity} and the {@link SqlDatePartFunction}s needed to group on
     * and read into a DateTime.
     *
     * @return the mapping between {@link Granularity} and {@link SqlDatePartFunction}s.
     */
    public static Map<Granularity, List<SqlDatePartFunction>> buildDefaultGranularityToDateFunctionsMap() {
        Map<Granularity, List<SqlDatePartFunction>> defaultMap = new HashMap<>();
        defaultMap.put(AllGranularity.INSTANCE, Collections.emptyList());
        defaultMap.put(DefaultTimeGrain.YEAR, asList(YEAR));
        defaultMap.put(DefaultTimeGrain.MONTH, asList(YEAR, MONTH));
        defaultMap.put(DefaultTimeGrain.WEEK, asList(YEAR, WEEK));
        defaultMap.put(DefaultTimeGrain.DAY, asList(YEAR, DAYOFYEAR));
        defaultMap.put(DefaultTimeGrain.HOUR, asList(YEAR, DAYOFYEAR, HOUR));
        defaultMap.put(DefaultTimeGrain.MINUTE, asList(YEAR, DAYOFYEAR, HOUR, MINUTE));

        return defaultMap;
    }

    /**
     * Gets a list of {@link SqlDatePartFunction} to be performed on a timestamp
     * which can be used to group by the given {@link Granularity}.
     *
     * @param granularity  The granularity to map to a list of {@link SqlDatePartFunction}.
     *
     * @return the list of sql functions.
     */
    public List<SqlDatePartFunction> timeGrainToDatePartFunctions(Granularity granularity) {
        if (granularity instanceof ZonedTimeGrain) {
            ZonedTimeGrain defaultTimeGrain = (ZonedTimeGrain) granularity;
            return granularityToDateFunctionMap.get(defaultTimeGrain.getBaseTimeGrain());
        }
        return granularityToDateFunctionMap.get(granularity);
    }

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
    public RexNode buildTimeFilters(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            String timestampColumn
    ) {
        // create filters to only select results within the given intervals
        List<RexNode> timeFilters = druidQuery.getIntervals().stream()
                .map(interval -> {

                    DateTimeZone timeZone = getTimeZone(druidQuery);
                    String start = TimestampUtils.dateTimeToString(
                            interval.getStart().toDateTime(timeZone),
                            timeStringFormat
                    );
                    String end = TimestampUtils.dateTimeToString(
                            interval.getEnd().toDateTime(timeZone),
                            timeStringFormat
                    );

                    return builder.and(
                            builder.call(
                                    SqlStdOperatorTable.GREATER_THAN,
                                    builder.field(timestampColumn),
                                    builder.literal(start)
                            ),
                            builder.call(
                                    SqlStdOperatorTable.LESS_THAN,
                                    builder.field(timestampColumn),
                                    builder.literal(end)
                            )
                    );
                })
                .collect(Collectors.toList());

        return builder.or(timeFilters);
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
    public List<RexNode> buildGroupBy(RelBuilder builder, Granularity granularity, String timeColumn) {
        List<SqlDatePartFunction> sqlDatePartFunctions = timeGrainToDatePartFunctions(granularity);
        if (sqlDatePartFunctions.isEmpty()) {
            return Collections.singletonList(builder.field(timeColumn));
        }

        return sqlDatePartFunctions
                .stream()
                .map(sqlDatePartFunction ->
                        builder.alias(builder.call(
                                sqlDatePartFunction,
                                builder.field(timeColumn)),
                                sqlDatePartFunction.getName()
                        )
                )
                .collect(Collectors.toList());
    }

    /**
     * Given an array of strings (a row from a {@link java.sql.ResultSet}) and the
     * {@link Granularity} used to make groupBy statements on time, it will parse out a {@link DateTime}
     * for the row which represents the beginning of the interval it was grouped on.
     *
     * @param offset the last column before the date fields.
     * @param recordValues  The results returned by Sql needed to read the time columns.
     * @param druidQuery  The original druid query which was made using calling
     * {@link #buildGroupBy(RelBuilder, Granularity, String)}.
     *
     * @return the datetime for the start of the interval.
     */
    public DateTime getIntervalStart(int offset, String[] recordValues, DruidAggregationQuery<?> druidQuery) {
        List<SqlDatePartFunction> times = timeGrainToDatePartFunctions(druidQuery.getGranularity());

        DateTimeZone timeZone = getTimeZone(druidQuery);

        if (times.isEmpty()) {
            throw new UnsupportedOperationException("Can't parse dateTime for if no times were grouped on.");
        }

        MutableDateTime mutableDateTime = new MutableDateTime(0, 1, 1, 0, 0, 0, 0, timeZone);

        for (int i = 0; i < times.size(); i++) {
            int value = Integer.parseInt(recordValues[offset + i]);
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
    private DateTimeZone getTimeZone(DruidAggregationQuery<?> druidQuery) {
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
    protected void setDateTime(int value, SqlDatePartFunction sqlDatePartFn, MutableDateTime dateTime) {
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
