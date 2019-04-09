// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.mappers;

import com.yahoo.bard.webservice.data.Result;
import com.yahoo.bard.webservice.data.ResultSetSchema;
import com.yahoo.bard.webservice.data.time.AllGranularity;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.Interval;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * A mapper that removes results which overlap a missing interval set.
 */
public class PartialDataResultSetMapper extends ResultSetMapper {
    final SimplifiedIntervalList missingIntervals;
    final Supplier<SimplifiedIntervalList> volatileIntervalSupply;

    /**
     * Build a mapper to filter results over missing intervals from a result set, retaining any that are also volatile.
     *
     * @param missingIntervals  A list of intervals which represent gaps in the complete data set
     * @param volatileIntervalSupply  A supplier for intervals which should not be removed, even if incomplete
     */
    public PartialDataResultSetMapper(
            SimplifiedIntervalList missingIntervals,
            Supplier<SimplifiedIntervalList> volatileIntervalSupply
    ) {
        this.missingIntervals = missingIntervals;
        this.volatileIntervalSupply = volatileIntervalSupply;
    }

    /**
     * Remove result records which are missing and not marked as volatile.
     * Any bucket which is partially volatile is not removed.  In the case of the All granularity, all data is
     * considered to be in a single bucket.
     *
     * @param result   The result row being transformed
     * @param schema   The schema for that result
     * @return Null if the bucket this result falls in is missing but not volatile
     */
    @Override
    public Result map(Result result, ResultSetSchema schema) {
        Granularity grain = schema.getGranularity();

        if (grain.equals(AllGranularity.INSTANCE)) {
            return ! volatileIntervalSupply.get().isEmpty() || missingIntervals.isEmpty() ?
                    result :
                    null;
        }

        // Currently any Granularity which isn't 'ALL' must currently be a TimeGrain
        Interval resultInterval = new Interval(result.getTimeStamp(), ((TimeGrain) grain).getPeriod());

        return getMissingNotVolatile().stream().anyMatch((it) -> it.overlaps(resultInterval)) ?
                null :
                result;
    }

    @Override
    protected ResultSetSchema map(ResultSetSchema schema) {
        return schema;
    }

    /**
     * Return the intervals which are missing but not volatile.
     * These intervals will be pruned from the result set.
     *
     * @return the simplified interval list of times which are to be filtered for partiality
     */
    private SimplifiedIntervalList getMissingNotVolatile() {
        return missingIntervals.subtract(volatileIntervalSupply.get());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        PartialDataResultSetMapper that = (PartialDataResultSetMapper) o;
        return super.equals(o) &&
                Objects.equals(missingIntervals, that.missingIntervals);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), missingIntervals);
    }
}
