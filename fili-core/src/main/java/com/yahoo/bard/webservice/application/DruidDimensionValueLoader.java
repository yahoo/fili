// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.time.AllGranularity;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource;
import com.yahoo.bard.webservice.druid.model.query.DruidSearchQuery;
import com.yahoo.bard.webservice.druid.model.query.RegexSearchQuerySpec;
import com.yahoo.bard.webservice.druid.model.query.SearchQuerySpec;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.web.handlers.RequestContext;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The DruidDimensionRowProvider sends requests to the druid search query interface to get a list of dimension
 * values to add to the dimension cache.
 */
public class DruidDimensionValueLoader implements DimensionValueLoader {
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    public static final String DRUID_DIM_LOADER_DIMENSIONS =
            SYSTEM_CONFIG.getPackageVariableName("druid_dim_loader_dimensions");
    public static final String DRUID_DIM_LOADER_ROW_LIMIT =
            SYSTEM_CONFIG.getPackageVariableName("druid_dim_loader_row_limit");

    public static final String DRUID_DIM_LOADER_LOOKBACK_PERIOD =
            SYSTEM_CONFIG.getPackageVariableName("druid_dim_loader_lookback_period");

    private static final Integer ROW_LIMIT = SYSTEM_CONFIG.getIntProperty(DRUID_DIM_LOADER_ROW_LIMIT, 1000);

    private static final Period LOOKBACK = new Period(SYSTEM_CONFIG.getStringProperty(
            DRUID_DIM_LOADER_LOOKBACK_PERIOD,
            "P10Y"
    ));

    private static final String ANY_MATCH_PATTERN = ".*";
    private static final SearchQuerySpec SEARCH_QUERY_SPEC = new RegexSearchQuerySpec(ANY_MATCH_PATTERN);

    private final DruidWebService druidWebService;
    private final LinkedHashSet<Dimension> dimensions;
    private final LinkedHashSet<DataSource> dataSources;

    private HttpErrorCallback errorCallback;
    private FailureCallback failureCallback;

    /**
     * DruidDimensionRowProvider fetches data from Druid and adds it to the dimension cache.
     * The dimensions loaded are taken from the system config.
     *
     * @param physicalTableDictionary  The physical tables
     * @param dimensionDictionary  The dimensions to update
     * @param druidWebService  The druid webservice to query
     */
    public DruidDimensionValueLoader(
            PhysicalTableDictionary physicalTableDictionary,
            DimensionDictionary dimensionDictionary,
            DruidWebService druidWebService
    ) {
        this(
                physicalTableDictionary,
                dimensionDictionary,
                //Our configuration framework automatically converts a comma-separated config value into a list.
                SYSTEM_CONFIG.getListProperty(DRUID_DIM_LOADER_DIMENSIONS),
                druidWebService
        );
    }

    /**
     * DruidDimensionRowProvider fetches data from Druid and adds it to the dimension cache.
     * The dimensions to be loaded can be passed in as a parameter.
     *
     * @param physicalTableDictionary  The physical tables
     * @param dimensionDictionary  The dimension dictionary to load dimensions from.
     * @param dimensionsToLoad  The dimensions to use.
     * @param druidWebService  The druid webservice to query.
     */
    public DruidDimensionValueLoader(
            PhysicalTableDictionary physicalTableDictionary,
            DimensionDictionary dimensionDictionary,
            List<String> dimensionsToLoad,
            DruidWebService druidWebService
    ) {
        this.dimensions = dimensionsToLoad.stream()
                .map(dimensionDictionary::findByApiName)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        this.dataSources = physicalTableDictionary.values().stream()
                .map(table -> table.withConstraint(DataSourceConstraint.unconstrained(table)))
                .map(TableDataSource::new)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        this.druidWebService = druidWebService;
    }

    @Override
    public void setErrorCallback(HttpErrorCallback errorCallback) {
        this.errorCallback = errorCallback;
    }

    @Override
    public void setFailureCallback(FailureCallback failureCallback) {
        this.failureCallback = failureCallback;
    }

    @Override
    public Set<Dimension> getDimensions() {
        return dimensions;
    }

    @Override
    public Set<DataSource> getDataSources() {
        return dataSources;
    }

    @Override
    public void query(Dimension dimension, DataSource dataSource) {
        // Success callback will update the dimension cache
        SuccessCallback success = buildDruidDimensionsSuccessCallback(dimension);

        Interval interval = new Interval(LOOKBACK, DateTime.now());

        DruidSearchQuery druidSearchQuery = new DruidSearchQuery(
                dataSource,
                AllGranularity.INSTANCE,
                null,
                Collections.singletonList(interval),
                Collections.singletonList(dimension),
                SEARCH_QUERY_SPEC,
                null,
                ROW_LIMIT
        );

        RequestContext requestContext = new RequestContext(null, false);
        druidWebService.postDruidQuery(
                requestContext,
                success,
                errorCallback,
                failureCallback,
                druidSearchQuery
        );
    }

    /**
     * Build the callback to handle the successful druid query response.
     *
     * @param dimension  Dimension for which we are getting values
     *
     * @return the callback
     */
    private SuccessCallback buildDruidDimensionsSuccessCallback(Dimension dimension) {
        return rootNode -> {
            rootNode.forEach(intervalNode -> {
                intervalNode.get("result").forEach(dim -> {
                    String value = dim.get("value").asText();
                    if (dimension.findDimensionRowByKeyValue(value) == null) {
                        DimensionRow dimRow = dimension.createEmptyDimensionRow(value);
                        updateDimensionWithValue(dimension, dimRow);
                    }
                });
            });

            updateDimension(dimension);
        };
    }
}
