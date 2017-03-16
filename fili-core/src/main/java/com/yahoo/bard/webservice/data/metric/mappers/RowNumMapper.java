// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.mappers;

import com.yahoo.bard.webservice.data.Result;
import com.yahoo.bard.webservice.data.ResultSet;
import com.yahoo.bard.webservice.data.ResultSetSchema;
import com.yahoo.bard.webservice.data.metric.MetricColumn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Mapper to add row numbers to each result in a result set.
 */
public class RowNumMapper extends ResultSetMapper {

    private static final String ROW_NUM_COLUMN_NAME = "rowNum";
    private static final Logger LOG = LoggerFactory.getLogger(RowNumMapper.class);

    @Override
    public ResultSet map(ResultSet resultSet) {

        ResultSetSchema schema = map(resultSet.getSchema());
        MetricColumn column = schema.getColumn(ROW_NUM_COLUMN_NAME, MetricColumn.class).get();

        int resultSetSize = resultSet.size();
        List<Result> newResults = new ArrayList<>(resultSetSize);
        for (int i = 0; i < resultSetSize; i++) {
            newResults.add(rowNumMap(resultSet.get(i), column, i));
        }

        ResultSet newResultSet = new ResultSet(schema, newResults);
        LOG.trace("Mapped resultSet: {} to new resultSet {}", resultSet, newResultSet);
        return newResultSet;
    }

    @Override
    protected Result map(Result result, ResultSetSchema schema) {
        throw new UnsupportedOperationException("This code should never be reached.");
    }

    @Override
    protected ResultSetSchema map(ResultSetSchema schema) {
        return schema.withAddColumn(new MetricColumn(ROW_NUM_COLUMN_NAME));
    }

    /**
     * Expand the result by adding a new metric column with the row number as the metric value.
     *
     * @param result  Result to expand
     * @param metricColumn  New column to add
     * @param rowNum  Row number to use as the metric value
     *
     * @return the expanded Result
     */
    private Result rowNumMap(Result result, MetricColumn metricColumn, int rowNum) {
        Map<MetricColumn, Object> metricValues = new LinkedHashMap<>(result.getMetricValues());
        metricValues.put(metricColumn, BigDecimal.valueOf(rowNum));

        return new Result(result.getDimensionRows(), metricValues, result.getTimeStamp());
    }
}
