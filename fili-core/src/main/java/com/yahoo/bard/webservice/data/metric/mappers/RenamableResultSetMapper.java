// Copyright 2020 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.mappers;

import com.yahoo.bard.webservice.data.ResultSet;

/**
 * {@link ResultSetMapper} that can be pointed at a different column in the {@link ResultSet} at query time. Any result
 * set mapper that is dependent on a specific column MUST implement this interface, otherwise renaming functionality
 * will not handle the ResultSetMapper properly.
 *
 * TODO if a single result set mapper needs to be able to rename multiple different columns, this interface needs to be
 *  expanded, as well as its interaction points.
 */
public interface RenamableResultSetMapper {

    /**
     * Re-point the result set mapper at {@code newColumnName}.
     *
     * @param newColumnName  The name of the new column to point at
     * @return  the re-pointed result set mapper
     */
    ResultSetMapper withColumnName(String newColumnName);
}
