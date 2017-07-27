// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource;
import com.yahoo.bard.webservice.druid.model.query.AllGranularity;
import com.yahoo.bard.webservice.druid.model.query.DruidSearchQuery;
import com.yahoo.bard.webservice.druid.model.query.RegexSearchQuerySpec;
import com.yahoo.bard.webservice.druid.model.query.SearchQuerySpec;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.web.handlers.RequestContext;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.inject.Singleton;

/**
 * DruidDimensionsLoader sends requests to the druid search query interface to get a list of dimension
 * values to add to the dimension cache.
 */
@Singleton
public class DruidDimensionsLoader extends Loader<Boolean> {

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    public static final String DRUID_DIM_LOADER_TIMER_DURATION_KEY =
            SYSTEM_CONFIG.getPackageVariableName("druid_dim_loader_timer_duration");
    public static final String DRUID_DIM_LOADER_TIMER_DELAY_KEY =
            SYSTEM_CONFIG.getPackageVariableName("druid_dim_loader_timer_delay");
    public static final String DRUID_DIM_LOADER_DIMENSIONS =
            SYSTEM_CONFIG.getPackageVariableName("druid_dim_loader_dimensions");
    public static final String DRUID_DIM_LOADER_ROW_LIMIT =
            SYSTEM_CONFIG.getPackageVariableName("druid_dim_loader_row_limit");

    private static final Logger LOG = LoggerFactory.getLogger(DruidDimensionsLoader.class);

    private static final long TEN_YEARS_MILLIS = 10 * 365 * 24 * 60 * 60 * 1000L;
    private static final String PATTERN = ".*";
    private static final Duration DURATION = new Duration(TEN_YEARS_MILLIS);
    private static final List<Interval> INTERVALS = Collections.singletonList(new Interval(DURATION, DateTime.now()));
    private static final SearchQuerySpec SEARCH_QUERY_SPEC = new RegexSearchQuerySpec(PATTERN);
    private static final Integer ROW_LIMIT = SYSTEM_CONFIG.getIntProperty(DRUID_DIM_LOADER_ROW_LIMIT, 1000);

    protected final HttpErrorCallback errorCallback;
    protected final FailureCallback failureCallback;

    private final DruidWebService druidWebService;
    private final AtomicReference<DateTime> lastRunTimestamp;
    private final List<Dimension> dimensions;
    private final List<DataSource> dataSources;

    /**
     * DruidDimensionsLoader fetches data from Druid and adds it to the dimension cache.
     * The dimensions loaded are taken from the system config.
     *
     * @param physicalTableDictionary  The physical tables
     * @param dimensionDictionary  The dimensions to update
     * @param druidWebService  The druid webservice to query
     */
    public DruidDimensionsLoader(
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
     * DruidDimensionsLoader fetches data from Druid and adds it to the dimension cache.
     * The dimensions to be loaded can be passed in as a parameter.
     *
     * @param physicalTableDictionary  The physical tables
     * @param dimensionDictionary  The dimension dictionary to load dimensions from.
     * @param dimensionsToLoad  The dimensions to use.
     * @param druidWebService  The druid webservice to query.
     */
    public DruidDimensionsLoader(
            PhysicalTableDictionary physicalTableDictionary,
            DimensionDictionary dimensionDictionary,
            List<String> dimensionsToLoad,
            DruidWebService druidWebService
    ) {
        super(
                DruidDimensionsLoader.class.getSimpleName(),
                SYSTEM_CONFIG.getLongProperty(DRUID_DIM_LOADER_TIMER_DELAY_KEY, 0),
                SYSTEM_CONFIG.getLongProperty(
                        DRUID_DIM_LOADER_TIMER_DURATION_KEY,
                        TimeUnit.MILLISECONDS.toMillis(60000)
                )
        );

        this.druidWebService = druidWebService;

        this.errorCallback = getErrorCallback();
        this.failureCallback = getFailureCallback();

        lastRunTimestamp = new AtomicReference<>();

        // A DruidSearchQuery requires a list of dimensions, which we would have to explicitly create at serialization
        // time if `dimensions` were a flat list instead of a list of singleton lists.
        this.dimensions = dimensionsToLoad.stream()
                .map(dimensionDictionary::findByApiName)
                .collect(Collectors.toList());

        this.dataSources = physicalTableDictionary.values().stream()
                .map(table -> table.withConstraint(DataSourceConstraint.unconstrained(table)))
                .map(TableDataSource::new)
                .collect(Collectors.toList());
    }


    @Override
    public void run() {
        dimensions.stream()
                .peek(dimension -> LOG.trace("Querying values for dimension: {}", dimension))
                .forEach(this::queryDruidDim);
        lastRunTimestamp.set(DateTime.now());
    }

    /**
     * Sets the success callback and makes a post query to druid for each data source.
     *
     * @param dimension The dimension to search
     */
    protected void queryDruidDim(Dimension dimension) {
        dataSources.stream()
                .filter(dataSource -> dimensionExistsInDataSource(dimension, dataSource))
                .forEach(dataSource -> {
                    DruidSearchQuery druidSearchQuery = new DruidSearchQuery(
                            dataSource,
                            AllGranularity.INSTANCE,
                            null,
                            INTERVALS,
                            Collections.singletonList(dimension),
                            SEARCH_QUERY_SPEC,
                            null,
                            ROW_LIMIT
                    );

                    // Success callback will update the dimension cache
                    SuccessCallback success = buildDruidDimensionsSuccessCallback(dimension);

                    RequestContext requestContext = new RequestContext(null, false);
                    druidWebService.postDruidQuery(
                            requestContext,
                            success,
                            errorCallback,
                            failureCallback,
                            druidSearchQuery
                    );
                });

    }

    /**
     * Checks if a {@link Dimension} exists in a {@link DataSource}.
     *
     * @param dimension  The dimension to look for in the datasource.
     * @param dataSource  The datasource to look through for the dimension.
     *
     * @return true if the dimension was found.
     */
    private boolean dimensionExistsInDataSource(Dimension dimension, DataSource dataSource) {
        return dataSource.getPhysicalTable()
                .getDimensions()
                .stream()
                .anyMatch(dimension::equals);
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
                        dimension.addDimensionRow(dimRow);
                    }
                });
            });

            // Tell the dimension it's been updated
            dimension.setLastUpdated(DateTime.now());
        };
    }

    public DateTime getLastRunTimestamp() {
        return lastRunTimestamp.get();
    }
}
