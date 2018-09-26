// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.mappers;

import com.yahoo.bard.webservice.data.Result;
import com.yahoo.bard.webservice.data.ResultSet;
import com.yahoo.bard.webservice.data.ResultSetSchema;

import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Mapper to truncate a result set produced by a groupby druid query to the requested top N rows.
 */
public class TopNResultSetMapper extends ResultSetMapper {
    private final int topN;

    /**
     * Constructor.
     *
     * @param topN  The N to use when truncating to the top N in a bucket
     */
    public TopNResultSetMapper(int topN) {
        this.topN = topN;
    }

    @Override
    public ResultSet map(ResultSet resultSet) {
        // TODO: Use only native stream operations in RxJava: GroupByTime -> Sort -> Take N -> Concat streams by time
        TopNAccumulator acc = new TopNAccumulator();
        resultSet.stream().forEachOrdered(acc);
        return new ResultSet(resultSet.getSchema(), acc.data);
    }

    @Override
    protected Result map(Result result, ResultSetSchema schema) {
        return result;
    }

    @Override
    protected ResultSetSchema map(ResultSetSchema schema) {
        return schema;
    }

    /**
     * Prunes the result set to at most top N rows per time bucket. It's designed to execute in order on a sequential
     * stream (i.e. using forEachOrdered)
     */
    private class TopNAccumulator implements Consumer<Result> {
        private int filledBuckets = 0;
        private DateTime recentTimeStamp = null;
        private final List<Result> data = new ArrayList<>();

        @Override
        public void accept(Result result) {
            DateTime timestamp = result.getTimeStamp();
            if (!timestamp.equals(recentTimeStamp)) {
                filledBuckets = 0;
                recentTimeStamp = timestamp;
            }

            if (filledBuckets++ < topN) {
                data.add(result);
            }
        }
    }
}
