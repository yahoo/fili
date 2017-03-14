// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.auto;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.TimeGrain;

import java.util.List;

/**
 *
 */
public interface DataSourceConfiguration {
    /**
     * Gets the name of a datasource as would be stored in Druid.
     * @return the name of the datasource.
     */
    String getName();

    /**
     * Gets the name of the datasource to be used as a {@link TableName} in fili.
     * @return the {@link TableName} for this datasource.
     */
    TableName getTableName();

    /**
     * Gets the names of all the metrics for the current datasource
     * @return a list of names of metrics for the current datasource.
     */
    List<String> getMetrics();

    /**
     * Gets the names of all the dimensions for the current datasource.
     * @return a list of names of dimensions for the current datasource.
     */
    List<String> getDimensions();

    /**
     * Gets the list of {@link TimeGrain} which are valid for use in queries.
     * @return a list of {@link TimeGrain} for the current datasource.
     */
    List<TimeGrain> getValidTimeGrains();
}
