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
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.druid.model.query.AllGranularity;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.Granularity;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlDatePartFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.joda.time.DateTimeZone;

import java.sql.Timestamp;
import java.util.Collections;
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
    private static final Map<Granularity, List<SqlDatePartFunction>> TIMEGRAIN_TO_GROUPBY = new
            HashMap<Granularity, List<SqlDatePartFunction>>() {
                {
                    put(AllGranularity.INSTANCE, Collections.emptyList());
                    put(DefaultTimeGrain.YEAR, asList(YEAR));
                    put(DefaultTimeGrain.MONTH, asList(YEAR, MONTH));
                    put(DefaultTimeGrain.WEEK, asList(YEAR, WEEK));
                    put(DefaultTimeGrain.DAY, asList(YEAR, DAYOFYEAR));
                    put(DefaultTimeGrain.HOUR, asList(YEAR, DAYOFYEAR, HOUR));
                    put(DefaultTimeGrain.MINUTE, asList(YEAR, DAYOFYEAR, HOUR, MINUTE));
                }
            };

    @Override
    public List<SqlDatePartFunction> timeGrainToDatePartFunctions(Granularity granularity) {
        if (granularity instanceof ZonedTimeGrain) {
            ZonedTimeGrain defaultTimeGrain = (ZonedTimeGrain) granularity;
            return TIMEGRAIN_TO_GROUPBY.get(defaultTimeGrain.getBaseTimeGrain());
        }
        return TIMEGRAIN_TO_GROUPBY.get(granularity);
    }

    @Override
    public RexNode buildTimeFilters(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            String timestampColumn
    ) {
        // create filters to only select results within the given intervals
        List<RexNode> timeFilters = druidQuery.getIntervals().stream()
                .map(interval -> {

                    DateTimeZone timeZone = getTimeZone(druidQuery);

                    Timestamp start = TimestampUtils.timestampFromDateTime(interval.getStart().toDateTime(timeZone));
                    Timestamp end = TimestampUtils.timestampFromDateTime(interval.getEnd().toDateTime(timeZone));

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
