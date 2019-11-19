// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * The DimensionValueLoader provides a way to update dimension values.
 */
public interface DimensionValueLoader {
    Logger LOG = LoggerFactory.getLogger(DimensionValueLoader.class);

    /**
     * Checks if a {@link Dimension} exists in a {@link DataSource}.
     *
     * @param dimension  The dimension to look for in the datasource.
     * @param dataSource  The datasource to look through for the dimension.
     *
     * @return true if the dimension was found.
     */
    default boolean dimensionExistsInDataSource(Dimension dimension, DataSource dataSource) {
        return dataSource.getPhysicalTable()
                .getDimensions()
                .stream()
                .anyMatch(dimension::equals);
    }

    /**
     * Start loading all dimensions.
     */
    default void load() {
        getDimensions().stream()
                .peek(dimension -> LOG.trace("Querying values for dimension: {}", dimension))
                .forEach(this::queryDimension);
    }

    /**
     * Queries a specific dimension. This only queries against a {@link DataSource} if it's table contains the given
     * {@link Dimension}.
     *
     * @param dimension  The dimension to load values for.
     */
    default void queryDimension(Dimension dimension) {
        getDataSources().stream()
                .filter(dataSource -> dimensionExistsInDataSource(dimension, dataSource))
                .forEach(dataSource -> query(dimension, dataSource));
    }

    /**
     * Queries for a specific {@link Dimension} against the given {@link DataSource}.
     *
     * @param dimension  The dimension to load.
     * @param dataSource  The datasource to query values for.
     */
    void query(Dimension dimension, DataSource dataSource);

    /**
     * Set a callback if an error occurs while querying.
     *
     * @param errorCallback  The callback to invoke on http errors.
     */
    void setErrorCallback(HttpErrorCallback errorCallback);

    /**
     * Set a callback if an exception occurs while querying.
     *
     * @param failureCallback  The callback to invoke on exceptions.
     */
    void setFailureCallback(FailureCallback failureCallback);

    /**
     * Gets the list of dimensions to load.
     *
     * @return the list of dimensions.
     */
    Set<Dimension> getDimensions();

    /**
     * Gets the list of datasources to query against.
     *
     * @return the list of datasources.
     */
    Set<DataSource> getDataSources();

    /**
     *  Tell the dimension it's been updated.
     *
     * @param dimension  The dimension to update saying it's been loaded.
     */
    default void updateDimension(Dimension dimension) {
        dimension.setLastUpdated(DateTime.now());
    }

    /**
     * Adds dimension row values to a dimension.
     *
     * @param dimension  The dimension to add the row to.
     * @param dimensionRow  The dimension row to be added.
     */
    default void updateDimensionWithValue(Dimension dimension, DimensionRow dimensionRow) {
        dimension.addDimensionRow(dimensionRow);
    }
}
