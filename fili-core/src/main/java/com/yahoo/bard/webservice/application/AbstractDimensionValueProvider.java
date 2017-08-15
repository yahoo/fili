// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * The AbstractDimensionRowProvider provides a way to load dimension rows onto dimensions.
 */
public abstract class AbstractDimensionValueProvider {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractDimensionValueProvider.class);

    private final List<Dimension> dimensions;
    private final List<DataSource> dataSources;

    private HttpErrorCallback errorCallback;
    private FailureCallback failureCallback;

    /**
     * DimensionLoader fetches data from Druid and adds it to the dimension cache.
     * The dimensions to be loaded can be passed in as a parameter.
     *
     * @param physicalTableDictionary  The physical tables
     * @param dimensionDictionary  The dimension dictionary to load dimensions from.
     * @param dimensionsToLoad  The dimensions to use.
     */
    public AbstractDimensionValueProvider(
            PhysicalTableDictionary physicalTableDictionary,
            DimensionDictionary dimensionDictionary,
            List<String> dimensionsToLoad
    ) {
        this.dimensions = dimensionsToLoad.stream()
                .map(dimensionDictionary::findByApiName)
                .collect(Collectors.toList());

        this.dataSources = physicalTableDictionary.values().stream()
                .map(table -> table.withConstraint(DataSourceConstraint.unconstrained(table)))
                .map(TableDataSource::new)
                .collect(Collectors.toList());
    }

    /**
     * Queries for a specific {@link Dimension} against the given {@link DataSource}.
     *
     * @param dimension  The dimension to load.
     * @param dataSource  The datasource to query values for.
     */
    protected abstract void query(Dimension dimension, DataSource dataSource);

    /**
     * Checks if a {@link Dimension} exists in a {@link DataSource}.
     *
     * @param dimension  The dimension to look for in the datasource.
     * @param dataSource  The datasource to look through for the dimension.
     *
     * @return true if the dimension was found.
     */
    protected boolean dimensionExistsInDataSource(Dimension dimension, DataSource dataSource) {
        return dataSource.getPhysicalTable()
                .getDimensions()
                .stream()
                .anyMatch(dimension::equals);
    }

    /**
     * Start loading all dimensions.
     */
    public void load() {
        dimensions.stream()
                .peek(dimension -> LOG.trace("Querying values for dimension: {}", dimension))
                .forEach(this::queryDimension);
    }

    /**
     * Queries a specific dimension. This only queries against a {@link DataSource} if it's table contains the given
     * {@link Dimension}.
     *
     * @param dimension  The dimension to load values for.
     */
    public void queryDimension(Dimension dimension) {
        getDataSources().stream()
                .filter(dataSource -> dimensionExistsInDataSource(dimension, dataSource))
                .forEach(dataSource -> query(dimension, dataSource));
    }

    /**
     * Set a callback if an error occurs while querying.
     *
     * @param errorCallback  The callback to invoke on http errors.
     */
    public void setErrorCallback(HttpErrorCallback errorCallback) {
        this.errorCallback = errorCallback;
    }

    /**
     * Gets the callback for handling http errors.
     *
     * @return the callback.
     */
    protected HttpErrorCallback getErrorCallback() {
        return errorCallback;
    }

    /**
     * Set a callback if an exception occurs while querying.
     *
     * @param failureCallback  The callback to invoke on exceptions.
     */
    public void setFailureCallback(FailureCallback failureCallback) {
        this.failureCallback = failureCallback;
    }

    /**
     * Gets the callback for handling exceptions.
     *
     * @return the callback.
     */
    protected FailureCallback getFailureCallback() {
        return failureCallback;
    }

    /**
     * Gets the list of dimensions to load.
     *
     * @return the list of dimensions.
     */
    protected List<Dimension> getDimensions() {
        return dimensions;
    }

    /**
     * Gets the list of datasources to query against.
     *
     * @return the list of datasources.
     */
    protected List<DataSource> getDataSources() {
        return dataSources;
    }

    /**
     *  Tell the dimension it's been updated.
     *
     * @param dimension  The dimension to update saying it's been loaded.
     */
    protected void provideLoadedDimension(Dimension dimension) {
        dimension.setLastUpdated(DateTime.now());
    }

    /**
     * Adds dimension row values to a dimension.
     *
     * @param dimension  The dimension to add the row to.
     * @param dimensionRow  The dimension row to be added.
     */
    protected void provideDimensionRow(Dimension dimension, DimensionRow dimensionRow) {
        dimension.addDimensionRow(dimensionRow);
    }
}
