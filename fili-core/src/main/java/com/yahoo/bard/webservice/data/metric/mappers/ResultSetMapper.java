// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.mappers;

import com.yahoo.bard.webservice.data.Result;
import com.yahoo.bard.webservice.data.ResultSet;
import com.yahoo.bard.webservice.data.ResultSetSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * ResultSetMapper is an abstract class for walking a result set.
 */
abstract public class ResultSetMapper {

    private static final Logger LOG = LoggerFactory.getLogger(ResultSetMapper.class);

    /**
     * Take a complete result set and replace it with one altered according to the rules of the concrete mapper.
     *
     * @param resultSet  The unmapped result set
     *
     * @return The mapped result set
     */
    public ResultSet map(ResultSet resultSet) {

        List<Result> newResults = new ArrayList<>();
        Result newResult;

        for (Result r: resultSet) {
            newResult = map(r, resultSet.getSchema());
            if (newResult != null) {
                newResults.add(newResult);
            }
        }

        ResultSetSchema newSchema = map(resultSet.getSchema());
        ResultSet newResultSet = new ResultSet(newSchema, newResults);
        LOG.trace("Mapped resultSet: {} to new resultSet {}", resultSet, newResultSet);

        return newResultSet;
    }

    /**
     * Returns a transformed result row, or null if the row is removed.
     *
     * @param result   The result row being transformed
     * @param schema   The schema for that result
     *
     * @return The result row, a modified copy, or null (if row is eliminated)
     */
    abstract protected Result map(Result result, ResultSetSchema schema);

    /**
     * Returns a transformed schema.
     *
     * @param schema  The schema being mapped
     *
     * @return The same schema or a new (altered) one
     */
    abstract protected ResultSetSchema map(ResultSetSchema schema);

    /**
     * Since a ResultSetMapper has no state associated with it, we consider two ResultSetMappers to be the same iff
     * they are of the same class.
     *
     * @param o  The object this is being compared to
     *
     * @return true if the objects have the same class, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
        return o != null && o.getClass() == getClass();
    }

    @Override
    public int hashCode() {
        return getClass().toString().length();
    }
}
