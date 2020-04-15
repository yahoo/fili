package com.yahoo.bard.webservice.data.metric.mappers;

import java.util.Map;

/**
 * Result set mapper that can be pointed at a different column in the result set at query time.
 *
 * TODO if a single result set mapper needs to be able to rename multiple different columns, this interface needs to be
 *  expanded, as well as its interaction points
 */
public interface RenamableResultSetMapper {

    /**
     * Rename the target metric column to {@code newColumnName}
     * @param newColumnName
     * @return
     */
    ResultSetMapper withColumnName(String newColumnName);
}
