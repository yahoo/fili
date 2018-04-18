// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.mappers;

/**
 * A ColumnMapper is a ResultSetMapper that operates on a single column.
 *
 * @deprecated with-like functionality no longer needed because delayed construction is being used instead
 */
@Deprecated
@FunctionalInterface
public interface ColumnMapper {

    /**
     * Returns a copy of this ResultSetMapper with the specified columnName.
     *
     * @param columnName  The name of column used to construct the mapper
     *
     * @return  A ResultSetMapper constructed using columnName
     */
    ResultSetMapper withColumnName(String columnName);
}
