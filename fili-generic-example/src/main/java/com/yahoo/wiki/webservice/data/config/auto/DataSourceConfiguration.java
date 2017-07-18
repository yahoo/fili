// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.auto;

import com.yahoo.fili.webservice.data.config.names.TableName;
import com.yahoo.fili.webservice.data.time.TimeGrain;

import java.util.List;

/**
 * Holds the minimum necessary configuration necessary to set up fili to
 * make requests to druid. This defines all metrics, dimensions, and *one*
 * valid time grain of a datasource.
 * Note the restriction to one time grain, a druid datasource could have
 * more than one, but it *should* have just a single grain.
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
     * Note: In theory, a Datasource in Druid could have more than 1 grain at
     * different times, but for now we only want to support one.
     *
     * @return a {@link TimeGrain} for the current table.
     */
    TimeGrain getValidTimeGrain();
}
