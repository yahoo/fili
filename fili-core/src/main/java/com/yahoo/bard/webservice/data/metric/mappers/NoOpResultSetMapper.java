// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.mappers;

import com.yahoo.bard.webservice.data.Result;
import com.yahoo.bard.webservice.data.ResultSet;
import com.yahoo.bard.webservice.table.Schema;

/**
 * NoOp Result set mapper.
 */
public class NoOpResultSetMapper extends ResultSetMapper implements ColumnMapper {
    @Override
    public ResultSet map(ResultSet resultSet) {
        return resultSet;
    }

    @Override
    protected Result map(Result result, Schema schema) {
        return result;
    }

    @Override
    protected Schema map(Schema schema) {
        return schema;
    }

    @Override
    @Deprecated
    public ResultSetMapper withColumnName(String newColumnName) {
        return this;
    }
}
