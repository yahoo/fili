// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.auto;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.TimeGrain;

import java.util.List;

/**
 * Holds the minimum necessary configuration necessary to set up fili to
 * make requests to druid. This defines all metrics, dimensions, and valid
 * time grains of a datasource.
 */
public interface DataSourceConfiguration {
    /**
     * Gets the name of a datasource as would be stored in Druid.
     *
     * @return the name of the datasource.
     */
    String getName();

    /**
     * Gets the name of the datasource to be used as a {@link TableName} in fili.
     *
     * @return the {@link TableName} for this datasource.
     */
    TableName getTableName();

    /**
     * Gets the names of all the metrics for the current datasource.
     *
     * @return a list of names of metrics for the current datasource.
     */
    List<String> getMetrics();

    /**
     * Gets the names of all the dimensions for the current datasource.
     *
     * @return a list of names of dimensions for the current datasource.
     */
    List<String> getDimensions();

    /**
     * Gets the {@link TimeGrain} which is valid for use in queries.
     *
     * @return a {@link TimeGrain} for the current table.
     */
    TimeGrain getValidTimeGrain();
}
