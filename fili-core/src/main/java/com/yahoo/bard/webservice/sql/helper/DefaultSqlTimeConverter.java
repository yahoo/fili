// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.helper;

import static java.util.Arrays.asList;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DAYOFYEAR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.HOUR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUTE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MONTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.WEEK;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.YEAR;

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import com.yahoo.bard.webservice.data.time.TimeGrain;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlDatePartFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.joda.time.Interval;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Handles converting between a {@link DefaultTimeGrain} and a list of
 * {@link SqlDatePartFunction} to create groupBy statements on intervals of time.
 */
public class DefaultSqlTimeConverter implements SqlTimeConverter {
    // This mapping shows what information we need to group for each granularity
    private static final Map<TimeGrain, List<SqlDatePartFunction>> TIMEGRAIN_TO_GROUPBY = new
            HashMap<TimeGrain, List<SqlDatePartFunction>>() {
                {
                    put(DefaultTimeGrain.YEAR, asList(YEAR));
                    put(DefaultTimeGrain.MONTH, asList(YEAR, MONTH));
                    put(DefaultTimeGrain.WEEK, asList(YEAR, WEEK));
                    put(DefaultTimeGrain.DAY, asList(YEAR, DAYOFYEAR));
                    put(DefaultTimeGrain.HOUR, asList(YEAR, DAYOFYEAR, HOUR));
                    put(DefaultTimeGrain.MINUTE, asList(YEAR, DAYOFYEAR, HOUR, MINUTE));
                }
            };

    @Override
    public List<SqlDatePartFunction> timeGrainToDatePartFunctions(TimeGrain timeGrain) {
        return TIMEGRAIN_TO_GROUPBY.get(timeGrain);
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
    @Override
    public RexNode buildTimeFilters(
            RelBuilder builder,
            Collection<Interval> intervals,
            String timestampColumn
    ) {
        // create filters to only select results within the given intervals
        List<RexNode> timeFilters = intervals.stream()
                .map(interval -> {
                    Timestamp start = TimestampUtils.timestampFromDateTime(interval.getStart());
                    Timestamp end = TimestampUtils.timestampFromDateTime(interval.getEnd());

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
