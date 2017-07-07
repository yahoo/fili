package com.yahoo.bard.webservice.sql.helper;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DAYOFYEAR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.HOUR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUTE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MONTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.WEEK;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.YEAR;

import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.druid.model.query.Granularity;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlDatePartFunction;
import org.apache.calcite.tools.RelBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

/**
 * Created by hinterlong on 7/7/17.
 */
public interface SqlTimeConverter {

    /**
     * Gets the number of {@link SqlDatePartFunction} used for the given {@link Granularity}.
     *
     * @param timeGrain  The timegrain to find groupBy functions for.
     *
     * @return the number of functions used to groupBy the given granularity.
     */
    int getNumberOfGroupByFunctions(TimeGrain timeGrain);

    /**
     * Builds a list of {@link RexNode} which will effectively groupBy the given {@link Granularity}.
     *
     * @param builder  The RelBuilder used with calcite to build queries.
     * @param timeGrain  The granularity to build the groupBy for.
     * @param timeColumn  The name of the timestamp column.
     *
     * @return the list of {@link RexNode} needed in the groupBy.
     */
    Stream<RexNode> buildGroupBy(
            RelBuilder builder,
            TimeGrain timeGrain,
            String timeColumn
    );

    List<SqlDatePartFunction> timeGrainToDatePartFunctions(TimeGrain timeGrain);

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
    RexNode buildTimeFilters(
            RelBuilder builder,
            Collection<Interval> intervals,
            String timestampColumn
    );

    /**
     * Given a {@link java.sql.ResultSet} and the {@link Granularity} used to make groupBy
     * statements on time, it will parse out a {@link DateTime} for one row which
     * represents the beginning of the interval it was grouped on.
     *
     * @param offset the last column before the date fields.
     * @param row  The results returned by Sql needed to read the time columns.
     * @param timeGrain  The granularity which was used when calling
     * {@link #buildGroupBy(RelBuilder, TimeGrain, String)}.
     *
     * @return the datetime for the start of the interval.
     */
    default DateTime getIntervalStart(int offset, String[] row, TimeGrain timeGrain) {
        DateTime resultTimeStamp = new DateTime(0, DateTimeZone.UTC);

        List<SqlDatePartFunction> times = timeGrainToDatePartFunctions(timeGrain);
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
     default DateTime setDateTime(int value, SqlDatePartFunction sqlDatePartFn, DateTime dateTime) {
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
         }
         throw new IllegalArgumentException("Can't set value " + value + " for " + sqlDatePartFn);
     }
}
